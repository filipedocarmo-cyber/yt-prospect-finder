import streamlit as st
from typing import Dict
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import json


def yt_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key)


def _safe_execute(request, context_label: str, retries: int = 3, backoff: float = 1.5):
    """Executa uma request da API com tratamento de erros e tentativas."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return request.execute()
        except HttpError as e:
            last_err = e
            try:
                err_json = json.loads(e.content.decode("utf-8"))
                reason = (err_json.get("error", {}).get("errors", [{}])[0].get("reason")
                          or err_json.get("error", {}).get("message"))
            except Exception:
                reason = str(e)
            if reason in {"quotaExceeded", "dailyLimitExceeded"}:
                st.error("⚠️ Quota da YouTube Data API esgotada hoje. Tente novamente mais tarde ou use outra API Key.")
                st.stop()
            elif reason in {"keyInvalid", "forbidden", "ipRefererBlocked"}:
                st.error("🔑 API Key inválida ou restrita (HTTP 403). Verifique se a YouTube Data API v3 está habilitada e se a chave permite este uso.")
                st.stop()
            elif reason in {"badRequest"}:
                st.error(f"❗ Requisição inválida ao buscar {context_label}. Verifique os parâmetros.")
                st.stop()
            time.sleep(backoff ** attempt)
        except Exception as e:
            last_err = e
            time.sleep(backoff ** attempt)
    st.error(f"Erro ao chamar a API para {context_label}: {last_err}")
    st.stop()


@st.cache_data(show_spinner=False)
def get_categories_map(api_key: str, region: str) -> Dict[str, str]:
    """Retorna um dict {categoryId: title} para a região."""
    service = yt_client(api_key)
    res = _safe_execute(
        service.videoCategories().list(part="snippet", regionCode=region),
        "categorias"
    )
    mapping = {}
    for it in res.get("items", []):
        if it.get("kind") == "youtube#videoCategory" and it.get("snippet", {}).get("assignable"):
            mapping[it.get("id")] = it.get("snippet", {}).get("title")
    return mapping


def search_videos(service, query, region, published_after_iso, limit=100):
    video_ids = []
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
            video_ids.append(item["id"]["videoId"])
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return video_ids


def get_videos_stats(service, ids_batch):
    res = _safe_execute(
        service.videos().list(part="snippet,statistics,contentDetails", id=",".join(ids_batch), maxResults=50),
        "videos",
    )
    return res.get("items", [])


def get_channels_stats(service, ids_batch):
    res = _safe_execute(
        service.channels().list(part="snippet,statistics", id=",".join(ids_batch), maxResults=50),
        "canais",
    )
    return res.get("items", [])
