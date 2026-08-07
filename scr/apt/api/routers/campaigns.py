"""Endpoints de campanhas: criar, listar, consultar status, pausar e retomar.

A campanha e a unidade de trabalho do sistema. O administrador diz "mande N
engajamentos para estas URLs a esta vazao", e o resto do sistema descobre como
fazer isso sem estourar limite de plataforma.

Um detalhe de projeto que aparece em `create_campaign`: a criacao e transacional
e a ativacao vem por ultimo. A campanha nasce `draft`, recebe o pool de
conteudos e so entao vira `active`. Como o dispatcher enxerga apenas campanhas
`active`, isso elimina a janela em que ele encontraria uma campanha ativa sem
nenhuma URL para enviar.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Path, Query, status

from apt.api.deps import PublisherDep
from apt.api.schemas import (
    CampaignActionOut,
    CampaignCreate,
    CampaignOut,
    CampaignStatusOut,
    ContentOut,
)
from apt.db.engine import connection
from apt.db.repositories import (
    CampaignRepository,
    ContentRepository,
    ExecutionRepository,
    TaskRepository,
)
from apt.domain.models import CampaignStatus, ControlMessage
from apt.logging_setup import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/campaigns", tags=["campanhas"])

CampaignId = Annotated[UUID, Path(description="Identificador da campanha")]


@router.post(
    "",
    response_model=CampaignOut,
    status_code=status.HTTP_201_CREATED,
    summary="Cria uma campanha",
)
async def create_campaign(payload: CampaignCreate) -> CampaignOut:
    """Cria a campanha, cadastra o pool de conteudos e (opcionalmente) ativa.

    As tres escritas acontecem na MESMA transacao. Se o cadastro dos conteudos
    falhar, a campanha nao fica gravada pela metade -- e uma campanha sem pool
    seria justamente o estado que o dispatcher nao sabe tratar.
    """
    async with connection() as conn:
        campaign_id = await CampaignRepository.create(
            conn,
            name=payload.name,
            platform=payload.platform,
            total_sends=payload.total_sends,
            target_rate_per_min=payload.target_rate_per_min,
            jitter_strategy=payload.jitter_strategy,
        )

        await ContentRepository.add_many(
            conn,
            campaign_id,
            [(item.url, item.weight) for item in payload.contents],
        )

        # Ativacao por ultimo: so agora existe pool para o dispatcher usar.
        if payload.activate:
            await CampaignRepository.set_status(conn, campaign_id, CampaignStatus.ACTIVE)

        created = await CampaignRepository.get(conn, campaign_id)

    if created is None:  # pragma: no cover - impossivel apos INSERT bem-sucedido
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="campanha criada mas nao encontrada na releitura",
        )

    logger.info(
        "api.campaign_created",
        campaign_id=str(campaign_id),
        platform=str(payload.platform),
        total_sends=payload.total_sends,
        target_rate_per_min=payload.target_rate_per_min,
        contents=len(payload.contents),
        activated=payload.activate,
    )
    return CampaignOut(**created)


@router.get("", response_model=list[CampaignOut], summary="Lista campanhas")
async def list_campaigns(
    status_filter: Annotated[
        CampaignStatus | None,
        Query(alias="status", description="Filtra por status"),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[CampaignOut]:
    """Lista campanhas, das mais recentes para as mais antigas."""
    async with connection() as conn:
        rows = await CampaignRepository.list_all(
            conn, status=status_filter, limit=limit, offset=offset
        )
    return [CampaignOut(**row) for row in rows]


@router.get("/{campaign_id}", response_model=CampaignOut, summary="Detalha uma campanha")
async def get_campaign(campaign_id: CampaignId) -> CampaignOut:
    async with connection() as conn:
        row = await CampaignRepository.get(conn, campaign_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"campanha {campaign_id} nao encontrada",
        )
    return CampaignOut(**row)


@router.get(
    "/{campaign_id}/status",
    response_model=CampaignStatusOut,
    summary="Status consolidado da campanha",
)
async def get_campaign_status(campaign_id: CampaignId) -> CampaignStatusOut:
    """Reune campanha, pool, contagem de tarefas e contagem de tentativas.

    O `outcome_breakdown` e a parte interessante para a POC: e onde se ve, para
    uma campanha especifica, quantos envios foram aceitos, quantos nos mesmos
    adiamos (`rate_limited_local`) e quantos a plataforma recusou (`throttled`).
    O objetivo do projeto e o ultimo numero ficar em zero.
    """
    async with connection() as conn:
        campaign = await CampaignRepository.get(conn, campaign_id)
        if campaign is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"campanha {campaign_id} nao encontrada",
            )
        contents = await ContentRepository.list_for_campaign(conn, campaign_id)
        task_breakdown = await TaskRepository.status_breakdown(conn, campaign_id)
        outcome_breakdown = await ExecutionRepository.outcome_breakdown(conn)

    total = int(campaign["total_sends"]) or 1
    progress = min(100.0, round(int(campaign["sent_count"]) / total * 100, 2))

    return CampaignStatusOut(
        campaign=CampaignOut(**campaign),
        contents=[ContentOut(**c) for c in contents],
        task_breakdown=task_breakdown,
        outcome_breakdown=outcome_breakdown,
        progress_percent=progress,
    )


@router.post(
    "/{campaign_id}/pause",
    response_model=CampaignActionOut,
    summary="Pausa uma campanha",
)
async def pause_campaign(campaign_id: CampaignId, publisher: PublisherDep) -> CampaignActionOut:
    """Pausa a materializacao de novas tarefas.

    Importante para nao gerar expectativa errada: pausar NAO cancela as tarefas
    ja publicadas na fila. Elas continuam sendo processadas -- o rate limiter e o
    circuit breaker continuam valendo para elas. Cancelar mensagens em voo
    exigiria purgar a fila, o que descartaria tambem tarefas de outras campanhas
    da mesma plataforma.

    O evento de controle e difundido por fanout para que todos os workers saibam
    da pausa e possam usar essa informacao (hoje, apenas para log).
    """
    async with connection() as conn:
        found = await CampaignRepository.set_status(conn, campaign_id, CampaignStatus.PAUSED)
    if not found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"campanha {campaign_id} nao encontrada",
        )

    await publisher.publish_control(
        ControlMessage(type="campaign_paused", payload={"campaign_id": str(campaign_id)})
    )
    logger.info("api.campaign_paused", campaign_id=str(campaign_id))

    return CampaignActionOut(
        id=campaign_id,
        status=CampaignStatus.PAUSED,
        message=(
            "Campanha pausada. Tarefas ja enfileiradas seguem sendo processadas "
            "sob as mesmas politicas de rate limit e circuit breaker."
        ),
    )


@router.post(
    "/{campaign_id}/resume",
    response_model=CampaignActionOut,
    summary="Retoma uma campanha pausada",
)
async def resume_campaign(campaign_id: CampaignId, publisher: PublisherDep) -> CampaignActionOut:
    """Volta a campanha para `active`.

    Recusa retomar uma campanha `completed`: o orcamento de envios acabou, e
    reativar faria o dispatcher encontra-la ativa com `dispatched_count >=
    total_sends` e ignora-la a cada tick -- um estado que parece funcionando e
    nao faz nada.
    """
    async with connection() as conn:
        current = await CampaignRepository.get(conn, campaign_id)
        if current is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"campanha {campaign_id} nao encontrada",
            )
        if current["status"] == CampaignStatus.COMPLETED:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "campanha concluida nao pode ser retomada: o orcamento de "
                    f"{current['total_sends']} envios foi consumido. Crie uma nova."
                ),
            )
        await CampaignRepository.set_status(conn, campaign_id, CampaignStatus.ACTIVE)

    await publisher.publish_control(
        ControlMessage(type="campaign_resumed", payload={"campaign_id": str(campaign_id)})
    )
    logger.info("api.campaign_resumed", campaign_id=str(campaign_id))

    return CampaignActionOut(
        id=campaign_id,
        status=CampaignStatus.ACTIVE,
        message="Campanha retomada.",
    )
