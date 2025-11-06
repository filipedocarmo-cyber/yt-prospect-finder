# Adicionei filtro de duração mínima de vídeo para excluir Shorts

"""
YT Prospect Finder — Canais Pequenos com Vídeos Virais (Streamlit)
-----------------------------------------------------------------
Agora com filtro para excluir vídeos curtos (ex: Shorts). O usuário pode escolher
uma duração mínima, como 10 minutos.
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
    raw_queries = st.text_area("Palavras‑chave", "historias emocionantes, desaparecidos, padre cícero")
    region = st.selectbox("Região", ["BR","US","MX","ES","FR","PL","IT","PT"], index=0)
    published_after = st.date_input("Publicado depois de", (datetime.utcnow()-timedelta(days=365)).date())
    min_views = st.select_slider("Mínimo de views", options=[100_000,200_000,500_000,1_000_000], value=200_000)
    max_subs = st.number_input("Máx. inscritos",1,100000,10000,500)
    min_duration = st.number_input("⏱️ Duração mínima (minutos)", min_value=0, max_value=60, value=10, step=1, help="Vídeos menores serão ignorados (exclui Shorts).")
    max_per_query = st.slider("Máx. vídeos por palavra‑chave",20,200,100,20)

@st.cache_data(show_spinner=False)
def yt_client(api_key:str):
    return build("youtube","v3",developerKey=api_key)

def chunked(lst:List[str], size:int)->List[List[str]]:
    return [lst[i:i+size] for i in range(0,len(lst),size)]

def safe_int(x:Any,default:int=0)->int:
    try: return int(x)
    except: return default

def parse_duration(duration_str:str)->float:
    try:
        td=isodate.parse_duration(duration_str)
        return td.total_seconds()/60
    except: return 0

def search_videos(service,query,region,published_after_iso,limit:int)->List[str]:
    video_ids=[]; page_token=None
    while len(video_ids)<limit:
        res=service.search().list(part="id",type="video",order="viewCount",q=query,regionCode=region,publishedAfter=published_after_iso,maxResults=min(50,limit-len(video_ids)),pageToken=page_token,safeSearch="none").execute()
        for item in res.get("items",[]):
            vid=item["id"].get("videoId");
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
            duration=parse_duration(cd.get("duration","PT0M"))
            rows.append({
                "videoId":it.get("id"),
                "title":sn.get("title"),
                "publishedAt":sn.get("publishedAt"),
                "channelId":sn.get("channelId"),
                "channelTitle_video":sn.get("channelTitle"),
                "views":safe_int(stt.get("viewCount")),
                "likes":safe_int(stt.get("likeCount")),
                "comments":safe_int(stt.get("commentCount")),
                "duration_min":duration
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

if st.button("🚀 Buscar canais agora",type="primary"):
    if not api_key:
        st.error("Informe sua YouTube API Key."); st.stop()

    service=yt_client(api_key)
    queries=[q.strip() for q in raw_queries.split(",") if q.strip()]
    published_after_iso=datetime.combine(published_after,datetime.min.time()).isoformat("T")+"Z"

    all_video_ids=[]; pb=st.progress(0.0,text="Buscando vídeos…")
    for i,q in enumerate(queries,start=1):
        vids=search_videos(service,q,region,published_after_iso,max_per_query)
        all_video_ids.extend(vids)
        pb.progress(i/len(queries),text=f"{q}: {len(vids)} vídeos")

    videos_df=get_videos_stats(service,all_video_ids)
    if videos_df.empty: st.warning("Nenhum vídeo encontrado."); st.stop()

    videos_df=videos_df[videos_df["views"]>=min_views]
    videos_df=videos_df[videos_df["duration_min"]>=min_duration]  # FILTRO DE DURAÇÃO

    if videos_df.empty: st.warning("Nenhum vídeo atende aos filtros (views e duração)."); st.stop()

    unique_channels=sorted(videos_df["channelId"].dropna().unique().tolist())
    ch_df=get_channels_stats(service,unique_channels)

    merged=videos_df.merge(ch_df,on=["channelId"],how="left",suffixes=("_video","_channel"))
    merged["channelTitle"]=merged.get("channelTitle_channel").fillna(merged.get("channelTitle_video"))
    merged=build_links(merged)
    merged["publishedAt"]=pd.to_datetime(merged["publishedAt"],errors="coerce")

    filtered=merged[(merged["subs"]>=0)&(merged["subs"]<=max_subs)]

    st.subheader("📋 Vídeos Encontrados")
    st.dataframe(filtered[["title","views","duration_min","publishedAt","channelTitle","subs","videoUrl"]],use_container_width=True)

    csv=filtered.to_csv(index=False)
    st.download_button("⬇️ Baixar CSV",data=csv,file_name="yt_videos_filtrados.csv",mime="text/csv")
else:
    st.info("Preencha a chave e clique em Buscar.")
