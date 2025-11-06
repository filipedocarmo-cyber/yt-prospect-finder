
"""
YT Prospect Finder — Canais Pequenos com Vídeos Virais (Streamlit)
-----------------------------------------------------------------
App para encontrar **canais pequenos (≤ X inscritos)** que têm **vídeos virais** (100k/200k/500k/1M+).
Compatível com Python 3.12+ e Streamlit Cloud.

Como rodar localmente (resumo):
  pip install -r requirements.txt
  python -m streamlit run app.py

Repositório/Cloud: basta ter `app.py` e `requirements.txt`.
"""

from datetime import datetime, timedelta
from typing import List, Any

import pandas as pd
import streamlit as st
from googleapiclient.discovery import build

# ------------------------- UI SETUP -------------------------
st.set_page_config(
    page_title="YT Prospect Finder — Canais Pequenos com Vídeos Virais",
    layout="wide",
)

st.title("🔎 YT Prospect Finder — Canais Pequenos com Vídeos Virais")
st.caption(
    "Encontre canais com até X inscritos que têm vídeos com alto número de visualizações."
)

with st.sidebar:
    st.header("Configurações")
    api_key = st.text_input(
        "YouTube API Key",
        type="password",
        help="Cole aqui sua chave de API do YouTube Data API v3.",
    )

    default_queries = (
        "historias emocionantes, casas abandonadas, padre cícero, desaparecidos, archivos del depredador"
    )
    raw_queries = st.text_area(
        "Palavras‑chave (separadas por vírgula)",
        value=default_queries,
        help="Vamos buscar vídeos ordenados por visualizações para cada termo.",
    )

    region = st.selectbox(
        "Região (regionCode)",
        options=[
            "BR", "US", "MX", "ES", "FR", "PL", "IT", "PT", "AR", "CO", "CL",
        ],
        index=0,
        help="Afeta relevância/idioma dos resultados.",
    )

    published_after = st.date_input(
        "Publicado depois de",
        value=(datetime.utcnow() - timedelta(days=365)).date(),
        help="Filtra por data de publicação (default: últimos 12 meses).",
    )

    min_views = st.select_slider(
        "Mínimo de views do vídeo",
        options=[100_000, 200_000, 500_000, 1_000_000],
        value=200_000,
    )

    max_subs = st.number_input(
        "Máx. inscritos do canal",
        min_value=1,
        value=10_000,
        step=500,
        help="Ex.: 10.000 para achar canais ainda pequenos.",
    )

    max_per_query = st.slider(
        "Máx. vídeos por palavra‑chave",
        min_value=20,
        max_value=200,
        value=100,
        step=20,
        help="Quantos vídeos coletar por termo (paginação).",
    )

    st.markdown("---")
    st.caption("Dicas: use 3–8 palavras‑chave por rodada para economizar cota.")

# ------------------------- HELPERS -------------------------

@st.cache_data(show_spinner=False)
def yt_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key)


def chunked(lst: List[str], size: int) -> List[List[str]]:
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def search_videos(service, query: str, region: str, published_after_iso: str, limit: int) -> List[str]:
    """Retorna uma lista de videoIds ordenados por viewCount para um termo."""
    video_ids: List[str] = []
    page_token = None

    while len(video_ids) < limit:
        try:
            req = service.search().list(
                part="id",
                type="video",
                order="viewCount",
                q=query,
                regionCode=region,
                publishedAfter=published_after_iso,
                maxResults=min(50, limit - len(video_ids)),
                pageToken=page_token,
                safeSearch="none",
            )
            res = req.execute()
        except Exception as e:
            st.warning(f"Falha em search para '{query}': {e}")
            break

        for item in res.get("items", []):
            vid = item["id"].get("videoId")
            if vid:
                video_ids.append(vid)

        page_token = res.get("nextPageToken")
        if not page_token:
            break

    return video_ids


def get_videos_stats(service, video_ids: List[str]) -> pd.DataFrame:
    rows = []
    for ids_batch in chunked(video_ids, 50):
        try:
            res = service.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(ids_batch),
                maxResults=50,
            ).execute()
        except Exception as e:
            st.warning(f"Falha em videos.list: {e}")
            continue

        for it in res.get("items", []):
            sn = it.get("snippet", {})
            stt = it.get("statistics", {})
            rows.append(
                {
                    "videoId": it.get("id"),
                    "title": sn.get("title"),
                    "publishedAt": sn.get("publishedAt"),
                    "channelId": sn.get("channelId"),
                    "channelTitle_video": sn.get("channelTitle"),
                    "views": safe_int(stt.get("viewCount")),
                    "likes": safe_int(stt.get("likeCount")),
                    "comments": safe_int(stt.get("commentCount")),
                }
            )
    return pd.DataFrame(rows)


def get_channels_stats(service, channel_ids: List[str]) -> pd.DataFrame:
    rows = []
    for ids_batch in chunked(channel_ids, 50):
        try:
            res = service.channels().list(
                part="snippet,statistics",
                id=",".join(ids_batch),
                maxResults=50,
            ).execute()
        except Exception as e:
            st.warning(f"Falha em channels.list: {e}")
            continue

        for it in res.get("items", []):
            stt = it.get("statistics", {})
            sn = it.get("snippet", {})
            rows.append(
                {
                    "channelId": it.get("id"),
                    "channelTitle_channel": sn.get("title"),
                    "subs": safe_int(stt.get("subscriberCount"), -1),
                    "videoCount": safe_int(stt.get("videoCount")),
                    "country": sn.get("country"),
                }
            )
    return pd.DataFrame(rows)


def build_links(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["videoUrl"] = df["videoId"].apply(lambda x: f"https://www.youtube.com/watch?v={x}")
    df["channelUrl"] = df["channelId"].apply(lambda x: f"https://www.youtube.com/channel/{x}")
    return df


# ------------------------- MAIN ACTION -------------------------

if st.button("🚀 Buscar canais agora", type="primary"):
    if not api_key:
        st.error("Informe sua YouTube API Key na barra lateral.")
        st.stop()

    service = yt_client(api_key)

    queries = [q.strip() for q in raw_queries.split(",") if q.strip()]
    st.write(
        f"**Consultas:** {', '.join(queries)} | **Região:** {region} | **Min views:** {min_views:,} | **Max subs:** {max_subs:,}"
    )

    published_after_iso = (
        datetime.combine(published_after, datetime.min.time()).isoformat("T") + "Z"
    )

    all_video_ids: List[str] = []
    pb = st.progress(0.0, text="Coletando vídeos por termo…")

    for i, q in enumerate(queries, start=1):
        vids = search_videos(service, q, region, published_after_iso, max_per_query)
        all_video_ids.extend(vids)
        pb.progress(i / max(1, len(queries)), text=f"{q}: {len(vids)} vídeos")

    pb.progress(1.0, text="Buscando estatísticas dos vídeos…")
    videos_df = get_videos_stats(service, all_video_ids)

    if videos_df.empty:
        st.warning(
            "Nenhum vídeo encontrado. Tente aumentar o período, mudar as palavras‑chave ou a região."
        )
        st.stop()

    # Filtra por views mínimas
    videos_df = videos_df[videos_df["views"] >= min_views]

    if videos_df.empty:
        st.warning("Nenhum vídeo acima do limite de views. Reduza o limite ou amplie o período.")
        st.stop()

    # Busca stats dos canais correspondentes
    unique_channels = (
        sorted(videos_df["channelId"].dropna().unique().tolist()) if "channelId" in videos_df.columns else []
    )
    ch_df = get_channels_stats(service, unique_channels) if unique_channels else pd.DataFrame()

    if ch_df.empty:
        st.warning("Não foi possível obter estatísticas dos canais.")
        st.stop()

    # Merge e filtro por inscritos
    merged = videos_df.merge(
        ch_df,
        on=["channelId"],
        how="left",
        suffixes=("_video", "_channel"),
    )

    # Unificar nome do canal (pós-merge cria channelTitle_video e channelTitle_channel)
    if "channelTitle_channel" in merged.columns or "channelTitle_video" in merged.columns:
        merged["channelTitle"] = merged.get("channelTitle_channel")
        merged["channelTitle"] = merged["channelTitle"].fillna(merged.get("channelTitle_video"))
    else:
        # fallback: se só existir 'channelTitle'
        if "channelTitle" not in merged.columns and "channelTitle_video" in merged.columns:
            merged["channelTitle"] = merged["channelTitle_video"]

    # Garantir colunas presentes
    for col, default in [("subs", -1), ("country", None)]:
        if col not in merged.columns:
            merged[col] = default

    filtered = merged[(merged["subs"] >= 0) & (merged["subs"] <= max_subs)]

    if filtered.empty:
        st.info("Nenhum canal pequeno com vídeos virais dentro dos limites definidos.")
        st.stop()

    # Enriquecimento e ordenação
    filtered = build_links(filtered)
    filtered["publishedAt"] = pd.to_datetime(filtered["publishedAt"], errors="coerce")
    filtered = filtered.sort_values(["views", "publishedAt"], ascending=[False, False])

    # Agregados por canal (para priorização)
    cols_group = [
        c
        for c in ["channelId", "channelTitle", "subs", "country"]
        if c in filtered.columns
    ]
    if not cols_group:
        st.info("Não foi possível preparar o resumo por canal (colunas ausentes).")
        st.stop()

    agg = (
        filtered.groupby(cols_group, dropna=False)
        .agg(top_video_views=("views", "max"), qty_100k_plus=("videoId", "count"))
        .reset_index()
        .sort_values(["qty_100k_plus", "top_video_views"], ascending=[False, False])
    )

    st.subheader("📈 Prioridade por Canal (resumo)")
    df_show = agg.assign(
        channelUrl=agg["channelId"].apply(lambda x: f"https://www.youtube.com/channel/{x}")
    )
    st.dataframe(df_show, use_container_width=True)

    st.subheader("📋 Vídeos Encontrados (detalhado)")
    cols_show = [
        "title",
        "views",
        "publishedAt",
        "channelTitle",  # coluna unificada
        "subs",
        "country",
        "videoUrl",
        "channelUrl",
    ]
    cols_show = [c for c in cols_show if c in filtered.columns]
    st.dataframe(filtered[cols_show], use_container_width=True)

    # Download CSVs
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_detailed = filtered[cols_show].to_csv(index=False)
    csv_agg = agg.to_csv(index=False)

    st.download_button(
        label="⬇️ Baixar CSV — Vídeos Detalhados",
        data=csv_detailed,
        file_name=f"yt_prospect_videos_{ts}.csv",
        mime="text/csv",
    )

    st.download_button(
        label="⬇️ Baixar CSV — Resumo por Canal",
        data=csv_agg,
        file_name=f"yt_prospect_channels_{ts}.csv",
        mime="text/csv",
    )

    with st.expander("ℹ️ Notas sobre cota da API e performance"):
        st.markdown(
            """
            - `search.list` retorna até 50 vídeos por página; aqui paginamos até o limite escolhido por termo.
            - `videos.list` e `channels.list` são chamados em lotes de 50 IDs para reduzir custo de cota.
            - Dica: rode com 3–5 palavras‑chave e limite de 60–100 vídeos por termo para ficar leve e rápido.
            - Para escalar, rode em batches ao longo do dia (limite de cota padrão é 10.000 unidades/diárias por projeto).
            """
        )
else:
    st.info(
        "Preencha a chave de API e as palavras‑chave, depois clique em **Buscar canais agora**."
    )
