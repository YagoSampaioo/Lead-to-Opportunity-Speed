# dashboard.py
import os
import datetime as dt
import pandas as pd
from supabase import create_client, Client
import streamlit as st

# Libs do Google
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- CONFIGURAÇÃO ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']

# --- FUNÇÕES DE BUSCA DE DADOS (as mesmas de antes) ---
# Usamos o cache do Streamlit para não buscar os dados toda hora
@st.cache_data(ttl=3600) # Armazena o resultado por 1 hora
def get_supabase_leads():
    """Busca os leads e suas datas de criação no Supabase."""
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    try:
        response = supabase.table('leads_data').select('email, created_at').execute()
        df_leads = pd.DataFrame(response.data)
        df_leads['created_at'] = pd.to_datetime(df_leads['created_at']).dt.tz_localize(None)
        return df_leads
    except Exception as e:
        st.error(f"Erro ao buscar dados do Supabase: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_google_calendar_events():
    """Busca eventos em TODAS as agendas."""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    try:
        service = build('calendar', 'v3', credentials=creds)
        calendar_list_result = service.calendarList().list().execute()
        calendars = calendar_list_result.get('items', [])
        all_events = []
        for calendar in calendars:
            calendar_id = calendar['id']
            time_min = (dt.datetime.now() - dt.timedelta(days=90)).isoformat() + 'Z'
            events_result = service.events().list(calendarId=calendar_id, timeMin=time_min, maxResults=500, singleEvents=True, orderBy='startTime').execute()
            all_events.extend(events_result.get('items', []))
        
        processed_events = []
        for event in all_events:
            if event.get('created') and event.get('attendees'):
                for attendee in event['attendees']:
                    if not attendee.get('self', False) and not attendee.get('resource', False):
                        processed_events.append({'attendee_email': attendee.get('email'), 'event_created_at': event.get('created')})
        
        df_events = pd.DataFrame(processed_events)
        df_events['event_created_at'] = pd.to_datetime(df_events['event_created_at']).dt.tz_localize(None)
        return df_events
    except Exception as e:
        st.error(f"Ocorreu um erro na API do Google Calendar: {e}")
        return pd.DataFrame()

# --- INTERFACE COM STREAMLIT ---

st.set_page_config(layout="wide")
st.title("📊 Dashboard de Performance: Leads vs. Agendamentos")
st.markdown("Análise do tempo médio entre a criação de um lead e o agendamento da primeira call.")

if st.button("Analisar Dados Agora"):
    with st.spinner("Buscando dados do Supabase e Google Calendar... Isso pode levar um momento."):
        df_leads = get_supabase_leads()
        df_events = get_google_calendar_events()

    if df_leads.empty or df_events.empty:
        st.warning("Não foram encontrados dados suficientes para a análise.")
    else:
        st.success("Dados carregados com sucesso!")

        # --- Lógica de Negócio (a mesma de antes) ---
        merged_df = pd.merge(df_leads, df_events, left_on='email', right_on='attendee_email', how='inner')
        merged_df = merged_df.sort_values(by='event_created_at', ascending=True)
        first_call_df = merged_df.drop_duplicates(subset='email', keep='first')
        
        if first_call_df.empty:
            st.warning("Nenhum lead encontrado com um evento de call agendado.")
        else:
            first_call_df['speed'] = first_call_df['event_created_at'] - first_call_df['created_at']
            first_call_df = first_call_df[first_call_df['speed'] >= pd.Timedelta(0)]
            
            if first_call_df.empty:
                st.warning("Nenhum lead com agendamento válido foi encontrado.")
            else:
                average_speed = first_call_df['speed'].mean()
                
                # --- EXIBIÇÃO DOS RESULTADOS ---
                st.header("Resultados Principais")
                
                total_seconds = average_speed.total_seconds()
                days = int(total_seconds // 86400)
                hours = int((total_seconds % 86400) // 3600)
                
                col1, col2 = st.columns(2)
                col1.metric("Lead to Opportunity Speed", f"{days} dias e {hours} horas")
                col2.metric("Total de Leads Convertidos", f"{len(first_call_df)} leads")

                st.header("Detalhes dos Leads Convertidos")
                st.markdown("A tabela abaixo é interativa. Você pode ordenar as colunas clicando no cabeçalho.")
                
                # Preparando DataFrame para exibição
                display_df = first_call_df[['email', 'created_at', 'event_created_at', 'speed']].copy()
                display_df.rename(columns={
                    'email': 'E-mail do Lead',
                    'created_at': 'Data de Criação do Lead',
                    'event_created_at': 'Data do Agendamento da Call',
                    'speed': 'Tempo de Conversão'
                }, inplace=True)
                
                # Formatando a coluna de tempo para ficar mais legível
                display_df['Tempo de Conversão'] = display_df['Tempo de Conversão'].apply(lambda x: f"{x.days} dias, {x.seconds//3600} horas")
                
                st.dataframe(display_df, use_container_width=True)