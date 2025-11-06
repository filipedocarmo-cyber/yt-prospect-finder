"""
YT Prospect Finder — Canais Pequenos com Vídeos Virais (Streamlit)
-----------------------------------------------------------------
- Agora com **seção Em Alta** na tela principal:
  - Canais **≤ 20.000 inscritos**
  - Vídeos **≥ 20 minutos**
  - **≥ 10.000 views nos últimos 7 dias** (proxy: vídeos publicados nos últimos 7 dias com views totais ≥ 10k)
- Mantém filtros gerais personalizáveis na barra lateral.

Obs.: A API pública do YouTube não retorna views por janela de 7 dias por vídeo (isso é do
YouTube Analytics e requer OAuth). Usamos um **proxy confiável**: vídeos publicados
nos **últimos 7 dias** com **views totais ≥ 10k** e que pertençam a **canais ≤ 20k inscritos**.
"""

from datetime import datetime, timedelta
from typing import List, Any
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
import isodate

st.set_page_config(page_title="YT Prospect Finder — Canais Pequenos com Vídeos Virais", layout="wide")
st.title("🔎 YT Prospect Finder — Canais Pequenos com Vídeos Virais")

with st.sidebar:
    st.header("Configurações")
    api_key = st.text_input("YouTube API Key", type="password")
    raw_queries = st.text_area(
        "Palavras‑chave",
        "historias emocionantes, desaparecidos, padre cícero",
        help="Separe por vírgula. Vamos buscar por views em cada termo.",
    )
    region = st.selectbox("Região", ["BR","US","MX","ES","FR","PL","IT","PT","AR","CO","CL"], index=0)
    published_after = st.date_input("Publicado depois de", (datetime.utcnow()-timedelta(days=365)).date())

    # Filtros gerais (para tabela padrão)
    min_views_general = st.select_slider("Mínimo de views (tabela geral)", options=[10_000,50_000,100_000,200_000,500_000,1_000_000], value=200_000)
    max_subs_general = st.number_input("Máx. inscritos (tabela geral)",1,200_000,10_000,500)
    min_duration_general = st.number_input("⏱️ Duração mínima (min) — geral", min_value=0, max_value=180, value=10, step=1)

    max_per_query = st.slider("Máx. vídeos por palavra‑chave",20,200,100,20)

    st.markdown("---")
    st.caption("A seção **Em Alta** usa critérios fixos: ≤20k inscritos, ≥20min, ≥10k views e publicação nos últimos 7 dias.")

@st.cache_data(show_spinner=False)
def yt_client(api_key:str):
    return build("youtube","v3",developerKey=api_key)

def chunked(lst:List[str], size:int)->List[List[str]]:
    return [lst[i:i+size] for i in range(0,len(lst),size)]

def safe_int(x:Any,default:int=0)->int:
    try: return int(x)
    except: return default

def parse_duration_minutes(duration_str:str)->float:
    try:
        td=isodate.parse_duration(duration_str)
        return td.total_seconds()/60
    except: return 0.0

def search_videos(service,query,region,published_after_iso,limit:int)->List[str]:
    video_ids=[]; page_token=None
    while len(video_ids)<limit:
        res=service.search().list(
            part="id",
            type="video",
            order="viewCount",
            q=query,
            regionCode=region,
            publishedAfter=published_after_iso,
            maxResults=min(50,limit-len(video_ids)),
            pageToken=page_token,
            safeSearch="none"
        ).execute()
        for item in res.get("items",[]):
            vid=item["id"].get("videoId")
            if vid: video_ids.append(vid)
        page_token=res.get("nextPageToken")
        if not page_token: break
    return video_ids

def get_videos_stats(service,video_ids:List[str])->pd.DataFrame:
    rows=[]
    for ids_batch in chunked(video_ids,50):
        res=service.videos().list(part="snippet,statistics,contentDetails",id=",".join(ids_batch),maxResults=50).execute()
        for it in res.get("items",[]):
            sn=it.get("snippet",{}); stt=it.get("statistics",{}); cd=it.get("contentDetails",{})
            rows.append({
                "videoId":it.get("id"),
                "title":sn.get("title"),
                "publishedAt":sn.get("publishedAt"),
                "channelId":sn.get("channelId"),
                "channelTitle_video":sn.get("channelTitle"),
                "views":safe_int(stt.get("viewCount")),
                "likes":safe_int(stt.get("likeCount")),
                "comments":safe_int(stt.get("commentCount")),
                "duration_min":parse_duration_minutes(cd.get("duration","PT0M"))
            })
    return pd.DataFrame(rows)

def get_channels_stats(service,channel_ids:List[str])->pd.DataFrame:
    rows=[]
    for ids_batch in chunked(channel_ids,50):
        res=service.channels().list(part="snippet,statistics",id=",".join(ids_batch),maxResults=50).execute()
        for it in res.get("items",[]):
            sn=it.get("snippet",{}); stt=it.get("statistics",{})
            rows.append({
                "channelId":it.get("id"),
                "channelTitle_channel":sn.get("title"),
                "subs":safe_int(stt.get("subscriberCount"),-1),
                "country":sn.get("country")
            })
    return pd.DataFrame(rows)

def build_links(df:pd.DataFrame)->pd.DataFrame:
    df=df.copy()
    df["videoUrl"]=df["videoId"].apply(lambda x:f"https://www.youtube.com/watch?v={x}")
    df["channelUrl"]=df["channelId"].apply(lambda x:f"https://www.youtube.com/channel/{x}")
    return df

# ====================== MAIN ======================
if st.button("🚀 Buscar canais agora",type="primary"):
    if not api_key:
        st.error("Informe sua YouTube API Key."); st.stop()

    service=yt_client(api_key)
    queries=[q.strip() for q in raw_queries.split(",") if q.strip()]
    published_after_iso=datetime.combine(published_after,datetime.min.time()).isoformat("T")+"Z"

    # 1) Buscar vídeos por termos
    all_video_ids=[]; pb=st.progress(0.0,text="Buscando vídeos…")
    for i,q in enumerate(queries,start=1):
        vids=search_videos(service,q,region,published_after_iso,max_per_query)
        all_video_ids.extend(vids)
        pb.progress(i/len(queries),text=f"{q}: {len(vids)} vídeos")

    videos_df=get_videos_stats(service,all_video_ids)
    if videos_df.empty:
        st.warning("Nenhum vídeo encontrado."); st.stop()

    # 2) Trazer dados dos canais e unificar dado
    unique_channels=sorted(videos_df["channelId"].dropna().unique().tolist())
    ch_df=get_channels_stats(service,unique_channels)

    merged=videos_df.merge(ch_df,on=["channelId"],how="left",suffixes=("_video","_channel"))
    merged["channelTitle"]=merged.get("channelTitle_channel").fillna(merged.get("channelTitle_video"))
    merged=build_links(merged)
    merged["publishedAt"]=pd.to_datetime(merged["publishedAt"],errors="coerce")

    # =================== Seção EM ALTA (topo) ===================
    NOW_UTC = datetime.utcnow()
    cutoff_7d = NOW_UTC - timedelta(days=7)

    trending = merged[
        (merged["subs"] >= 0) & (merged["subs"] <= 20_000) &
        (merged["duration_min"] >= 20) &
        (merged["views"] >= 10_000) &
        (merged["publishedAt"] >= cutoff_7d)
    ].copy()

    trending = trending.sort_values(["views","publishedAt"], ascending=[False, False])

    st.subheader("🔥 Em Alta (últimos 7 dias)")
    st.caption("Canais ≤ 20k inscritos • Vídeos ≥ 20 min • ≥ 10k views • Publicados nos últimos 7 dias")

    if trending.empty:
        st.info("Nenhum vídeo em alta dentro desses critérios nesta busca. Tente outros termos/regiões.")
    else:
        cols_trend = [
            "title","views","duration_min","publishedAt","channelTitle","subs","videoUrl","channelUrl"
        ]
        cols_trend = [c for c in cols_trend if c in trending.columns]
        st.dataframe(trending[cols_trend], use_container_width=True)

    st.markdown("---")

    # =================== Tabela geral (com filtros do usuário) ===================
    general = merged.copy()

    # Aplicar filtros gerais
    general = general[(general["views"] >= min_views_general) & (general["duration_min"] >= min_duration_general)]
    general = general[(general["subs"] >= 0) & (general["subs"] <= max_subs_general)]

    if general.empty:
        st.warning("Nenhum vídeo atende aos filtros gerais definidos na barra lateral.")
        st.stop()

    general = general.sort_values(["views","publishedAt"], ascending=[False, False])

    st.subheader("📋 Vídeos Encontrados (tabela geral)")
    cols_gen = [
        "title","views","duration_min","publishedAt","channelTitle","subs","videoUrl","channelUrl"
    ]
    cols_gen = [c for c in cols_gen if c in general.columns]
    st.dataframe(general[cols_gen], use_container_width=True)

    # Downloads
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    st.download_button("⬇️ CSV — Em Alta (7d)", data=trending[cols_trend].to_csv(index=False) if not trending.empty else "",
                       file_name=f"yt_trending_7d_{ts}.csv", mime="text/csv", disabled=trending.empty)
    st.download_button("⬇️ CSV — Tabela Geral", data=general[cols_gen].to_csv(index=False),
                       file_name=f"yt_general_{ts}.csv", mime="text/csv")
else:
    st.info("Preencha a chave, defina suas palavras‑chave e clique em **Buscar canais agora**.")
