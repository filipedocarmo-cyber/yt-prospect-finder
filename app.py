"""
YT Prospect Finder — Canais Pequenos com Vídeos Virais (Streamlit)
-----------------------------------------------------------------
- 🔥 Em Alta configurável (máx. inscritos, mín. duração, mín. views, janela X dias)
- 🟢 Toggle “Somente Em Alta”
- 🗂️ Filtro de Categoria (por região)
- 🖼️ Miniaturas nas tabelas
- ✅ Tratamento de erros (quota/key), datas normalizadas, downloads CSV

Requisitos (requirements.txt):
  streamlit==1.37.1
  google-api-python-client
  pandas
  python-dateutil
  isodate
"""

from datetime import datetime, timedelta
from typing import List, Any, Dict

import json
import time

import isodate
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========================= Helpers API ========================= #

def yt_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key)


def _safe_execute(request, context_label: str, retries: int = 3, backoff: float = 1.5):
    """Executa request da API com tratamento de erros e tentativas exponenciais."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return request.execute()
        except HttpError as e:
            last_err = e
            try:
                err_json = json.loads(e.content.decode("utf-8"))
                reason = (
                    err_json.get("error", {}).get("errors", [{}])[0].get("reason")
                    or err_json.get("error", {}).get("message")
                )
            except Exception:
                reason = str(e)
            if reason in {"quotaExceeded", "dailyLimitExceeded"}:
                st.error("⚠️ Quota da YouTube Data API esgotada hoje. Use outra API Key ou aguarde a renovação da cota.")
                st.stop()
            elif reason in {"keyInvalid", "forbidden", "ipRefererBlocked"}:
                st.error("🔑 API Key inválida/restrita (403). Habilite a **YouTube Data API v3** e revise as restrições da chave.")
                st.stop()
            elif reason in {"badRequest"}:
                st.error(f"❗ Requisição inválida ao buscar {context_label}. Verifique parâmetros.")
                st.stop()
            time.sleep(backoff ** attempt)
        except Exception as e:
            last_err = e
            time.sleep(backoff ** attempt)
    st.error(f"Erro ao chamar a API para {context_label}: {last_err}")
    st.stop()


@st.cache_data(show_spinner=False)
def get_categories_map(api_key: str, region: str) -> Dict[str, str]:
    """Retorna {categoryId: title} para a região."""
    service = yt_client(api_key)
    res = _safe_execute(
        service.videoCategories().list(part="snippet", regionCode=region),
        "categorias",
    )
    mapping: Dict[str, str] = {}
    for it in res.get("items", []):
        if it.get("kind") == "youtube#videoCategory" and it.get("snippet", {}).get("assignable"):
            mapping[it.get("id")] = it.get("snippet", {}).get("title")
    return mapping


def chunked(lst: List[str], size: int) -> List[List[str]]:
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def parse_duration_minutes(duration_str: str) -> float:
    try:
        td = isodate.parse_duration(duration_str)
        return td.total_seconds() / 60
    except Exception:
        return 0.0


def search_videos(service, query: str, region: str, published_after_iso: str, limit: int) -> List[str]:
    video_ids: List[str] = []
    page_token = None
    while len(video_ids) < limit:
        res = _safe_execute(
            service.search().list(
                part="id",
                type="video",
                order="viewCount",
                q=query,
                regionCode=region,
                publishedAfter=published_after_iso,
                maxResults=min(50, limit - len(video_ids)),
                pageToken=page_token,
                safeSearch="none",
            ),
            f"search: '{query}'",
        )
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
        res = _safe_execute(
            service.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(ids_batch),
                maxResults=50,
            ),
            "videos",
        )
        for it in res.get("items", []):
            sn = it.get("snippet", {})
            stt = it.get("statistics", {})
            cd = it.get("contentDetails", {})
            th = sn.get("thumbnails", {})
            thumb = th.get("high", th.get("medium", th.get("default", {}))).get("url")
            rows.append(
                {
                    "videoId": it.get("id"),
                    "title": sn.get("title"),
                    "publishedAt": sn.get("publishedAt"),
                    "channelId": sn.get("channelId"),
                    "channelTitle_video": sn.get("channelTitle"),
                    "categoryId": sn.get("categoryId"),
                    "views": safe_int(stt.get("viewCount")),
                    "likes": safe_int(stt.get("likeCount")),
                    "comments": safe_int(stt.get("commentCount")),
                    "duration_min": parse_duration_minutes(cd.get("duration", "PT0M")),
                    "thumbnail": thumb,
                }
            )
    return pd.DataFrame(rows)


def get_channels_stats(service, channel_ids: List[str]) -> pd.DataFrame:
    rows = []
    for ids_batch in chunked(channel_ids, 50):
        res = _safe_execute(
            service.channels().list(
                part="snippet,statistics",
                id=",".join(ids_batch),
                maxResults=50,
            ),
            "canais",
        )
        for it in res.get("items", []):
            sn = it.get("snippet", {})
            stt = it.get("statistics", {})
            rows.append(
                {
                    "channelId": it.get("id"),
                    "channelTitle_channel": sn.get("title"),
                    "subs": safe_int(stt.get("subscriberCount"), -1),
                    "country": sn.get("country"),
                }
            )
    return pd.DataFrame(rows)


def build_links(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["videoUrl"] = df["videoId"].apply(lambda x: f"https://www.youtube.com/watch?v={x}")
    df["channelUrl"] = df["channelId"].apply(lambda x: f"https://www.youtube.com/channel/{x}")
    return df

# ========================= UI / Página ========================= #

st.set_page_config(page_title="YT Prospect Finder — Canais Pequenos com Vídeos Virais", layout="wide")
st.title("🔎 YT Prospect Finder — Canais Pequenos com Vídeos Virais")

with st.sidebar:
    st.header("Configurações")
    api_key = st.text_input("YouTube API Key", type="password")

    raw_queries = st.text_area(
        "Palavras‑chave",
        "historias emocionantes, desaparecidos, padre cícero",
        help="Separe por vírgula. Buscamos por views em cada termo.",
    )

    region = st.selectbox(
        "Região",
        ["BR", "US", "MX", "ES", "FR", "PL", "IT", "PT", "AR", "CO", "CL"],
        index=0,
    )

    published_after = st.date_input(
        "Publicado depois de",
        (datetime.utcnow() - timedelta(days=365)).date(),
    )

    min_views_general = st.select_slider(
        "Mínimo de views (tabela geral)",
        options=[10_000, 50_000, 100_000, 200_000, 500_000, 1_000_000],
        value=200_000,
    )
    max_subs_general = st.number_input("Máx. inscritos (tabela geral)", 1, 200_000, 10_000, 500)
    min_duration_general = st.number_input("⏱️ Duração mínima (min) — geral", 0, 180, 10, 1)

    max_per_query = st.slider("Máx. vídeos por palavra‑chave", 20, 200, 100, 20)

    st.markdown("---")
    st.subheader("Parâmetros — Em Alta")
    max_subs_hot = st.number_input("Máx. inscritos (Em Alta)", 1000, 200_000, 20_000, 500)
    min_dur_hot = st.number_input("⏱️ Duração mínima (min) (Em Alta)", 1, 240, 20, 1)
    min_views_hot = st.number_input(
        "Mín. views totais (Em Alta)", 1_000, 5_000_000, 10_000, 1000,
        help="Proxy para views na janela definida",
    )
    days_window = st.number_input("Janela (dias) (Em Alta)", 1, 30, 7, 1)
    show_only_trending = st.toggle("👀 Somente Em Alta", value=False, help="Esconde a tabela geral")

    st.markdown("---")
    st.subheader("Filtro de Categoria")
    st.caption("Categorias oficiais do YouTube para a região selecionada")

# Botão principal
clicked = st.button("🚀 Buscar canais agora", type="primary")

if clicked:
    if not api_key:
        st.error("Informe sua YouTube API Key.")
        st.stop()

    # Carrega categorias da região e oferece select no sidebar
    categories_map = get_categories_map(api_key, region)
    categories_titles = ["(Qualquer)"] + sorted(categories_map.values())
    selected_category_title = st.sidebar.selectbox("Categoria do vídeo", categories_titles, index=0)
    selected_category_id = None
    if selected_category_title != "(Qualquer)":
        selected_category_id = {v: k for k, v in categories_map.items()}.get(selected_category_title)

    # Monta cliente e executa buscas
    service = yt_client(api_key)

    queries = [q.strip() for q in raw_queries.split(",") if q.strip()]
    published_after_iso = (
        datetime.combine(published_after, datetime.min.time()).isoformat("T") + "Z"
    )

    all_video_ids: List[str] = []
    pb = st.progress(0.0, text="Buscando vídeos…")
    for i, q in enumerate(queries, start=1):
        vids = search_videos(service, q, region, published_after_iso, max_per_query)
        all_video_ids.extend(vids)
        pb.progress(i / max(1, len(queries)), text=f"{q}: {len(vids)} vídeos")

    videos_df = get_videos_stats(service, all_video_ids)
    if videos_df.empty:
        st.warning("Nenhum vídeo encontrado.")
        st.stop()

    # Filtro por categoria (se escolhido)
    if selected_category_id is not None and "categoryId" in videos_df.columns:
        videos_df = videos_df[videos_df["categoryId"] == selected_category_id]
        if videos_df.empty:
            st.info("Nenhum vídeo na categoria selecionada para esta busca.")
            st.stop()

    unique_channels = (
        sorted(videos_df["channelId"].dropna().unique().tolist())
        if "channelId" in videos_df.columns
        else []
    )
    ch_df = get_channels_stats(service, unique_channels) if unique_channels else pd.DataFrame()

    merged = videos_df.merge(
        ch_df, on=["channelId"], how="left", suffixes=("_video", "_channel")
    )
    merged["channelTitle"] = merged.get("channelTitle_channel").fillna(
        merged.get("channelTitle_video")
    )
    merged = build_links(merged)

    # Datas seguras
    merged["publishedAt"] = pd.to_datetime(merged["publishedAt"], errors="coerce")
    merged = merged.dropna(subset=["publishedAt"]).copy()
    merged["publishedAt"] = merged["publishedAt"].dt.tz_localize(None)

    # ----------------- Seção Em Alta ----------------- #
    NOW_UTC = datetime.utcnow()
    cutoff = NOW_UTC - timedelta(days=int(days_window))

    trending = merged[
        (merged["subs"] >= 0)
        & (merged["subs"] <= int(max_subs_hot))
        & (merged["duration_min"] >= float(min_dur_hot))
        & (merged["views"] >= int(min_views_hot))
        & (merged["publishedAt"] >= cutoff)
    ].copy()

    # Métrica de velocidade (views/dia) para ordenação alternativa
    delta_days = (NOW_UTC - trending["publishedAt"]).dt.total_seconds() / 86400.0
    trending["views_per_day"] = (trending["views"] / delta_days.replace(0, 0.0001)).round(1)

    trending = trending.sort_values(["views_per_day", "views"], ascending=[False, False])

    st.subheader("🔥 Em Alta (janela configurável)")
    st.caption(
        f"Canais ≤ {max_subs_hot:,} inscritos • Vídeos ≥ {min_dur_hot} min • ≥ {min_views_hot:,} views • Publicados nos últimos {int(days_window)} dias"
    )

    if trending.empty:
        st.info("Nenhum vídeo em alta dentro desses critérios nesta busca. Tente outros termos/regiões.")
    else:
        cols_trend = [
            "thumbnail",
            "title",
            "views",
            "views_per_day",
            "duration_min",
            "publishedAt",
            "channelTitle",
            "subs",
            "videoUrl",
            "channelUrl",
        ]
        cols_trend = [c for c in cols_trend if c in trending.columns]
        st.dataframe(
            trending[cols_trend],
            use_container_width=True,
            column_config={
                "thumbnail": st.column_config.ImageColumn("Thumb", width="small"),
                "views": st.column_config.NumberColumn("Views", format=","),
                "views_per_day": st.column_config.NumberColumn("Views/dia", format=",.1f"),
                "duration_min": st.column_config.NumberColumn("Min", format=",.1f"),
            },
        )

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        "⬇️ CSV — Em Alta",
        data=trending.to_csv(index=False) if not trending.empty else "",
        file_name=f"yt_trending_{ts}.csv",
        mime="text/csv",
        disabled=trending.empty,
    )

    st.markdown("---")

    # ----------------- Tabela Geral ----------------- #
    if not show_only_trending:
        general = merged.copy()
        general = general[
            (general["views"] >= int(min_views_general))
            & (general["duration_min"] >= float(min_duration_general))
        ]
        general = general[(general["subs"] >= 0) & (general["subs"] <= int(max_subs_general))]

        if general.empty:
            st.warning("Nenhum vídeo atende aos filtros gerais definidos na barra lateral.")
            st.stop()

        # Ordena por Views e Recência
        general = general.sort_values(["views", "publishedAt"], ascending=[False, False])

        st.subheader("📋 Vídeos Encontrados (tabela geral)")
        cols_gen = [
            "thumbnail",
            "title",
            "views",
            "duration_min",
            "publishedAt",
            "channelTitle",
            "subs",
            "videoUrl",
            "channelUrl",
        ]
        cols_gen = [c for c in cols_gen if c in general.columns]
        st.dataframe(
            general[cols_gen],
            use_container_width=True,
            column_config={
                "thumbnail": st.column_config.ImageColumn("Thumb", width="small"),
                "views": st.column_config.NumberColumn("Views", format=","),
                "duration_min": st.column_config.NumberColumn("Min", format=",.1f"),
            },
        )

        st.download_button(
            "⬇️ CSV — Tabela Geral",
            data=general.to_csv(index=False),
            file_name=f"yt_general_{ts}.csv",
            mime="text/csv",
        )
else:
    st.info("Preencha a chave, defina suas palavras‑chave e clique em **Buscar canais agora**.")
