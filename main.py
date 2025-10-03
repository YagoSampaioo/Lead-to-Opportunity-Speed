import os
import datetime as dt
from dotenv import load_dotenv
import pandas as pd
from supabase import create_client, Client

# Libs do Google
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# --- CONFIGURAÇÃO ---
# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Google Calendar API
# Se modificar esses escopos, delete o arquivo token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']

# --- FUNÇÕES ---

def get_supabase_leads():
    """Busca os leads e suas datas de criação no Supabase."""
    print("Buscando leads no Supabase...")
    try:
        # ATENÇÃO: Altere 'sua_tabela_leads' para o nome real da sua tabela.
        # Altere 'email' e 'created_at' para os nomes reais das suas colunas.
        response = supabase.table('leads_data').select('email, created_at').execute()
        
        leads_data = response.data
        if not leads_data:
            print("Nenhum lead encontrado no Supabase.")
            return pd.DataFrame()

        # Converte para DataFrame do Pandas
        df_leads = pd.DataFrame(leads_data)
        # Converte a coluna de data para o formato datetime (removendo timezone para facilitar a comparação)
        df_leads['created_at'] = pd.to_datetime(df_leads['created_at']).dt.tz_localize(None)
        
        print(f"Encontrados {len(df_leads)} leads.")
        return df_leads

    except Exception as e:
        print(f"Erro ao buscar dados do Supabase: {e}")
        return pd.DataFrame()

def get_google_calendar_events():
    """Busca eventos e suas datas de criação em TODAS as agendas do usuário."""
    print("Buscando eventos no Google Calendar...")
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

        # 1. PEGAR A LISTA DE TODAS AS AGENDAS
        print("Buscando a lista de agendas disponíveis...")
        calendar_list_result = service.calendarList().list().execute()
        calendars = calendar_list_result.get('items', [])
        
        all_events = [] # Lista para armazenar eventos de TODAS as agendas

        # 2. FAZER UM LOOP EM CADA AGENDA PARA BUSCAR OS EVENTOS
        for calendar in calendars:
            calendar_id = calendar['id']
            print(f"  -> Buscando eventos na agenda: {calendar.get('summary')} ({calendar_id})")

            # Busca eventos dos últimos 90 dias (ajuste conforme necessário)
            time_min = (dt.datetime.now() - dt.timedelta(days=90)).isoformat() + 'Z'

            # A requisição de eventos agora é feita dentro do loop
            events_result = service.events().list(
                calendarId=calendar_id, 
                timeMin=time_min,
                maxResults=500, # Aumente se tiver muitos eventos
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            all_events.extend(events) # Adiciona os eventos encontrados à lista principal

        if not all_events:
            print("Nenhum evento encontrado em nenhuma das agendas.")
            return pd.DataFrame()

        # 3. PROCESSAR A LISTA UNIFICADA DE EVENTOS
        processed_events = []
        for event in all_events:
            creation_date = event.get('created')
            if not creation_date:
                continue
            
            attendees = event.get('attendees', [])
            for attendee in attendees:
                if not attendee.get('self', False) and not attendee.get('resource', False):
                    processed_events.append({
                        'attendee_email': attendee.get('email'),
                        'event_created_at': creation_date
                    })
        
        df_events = pd.DataFrame(processed_events)
        df_events['event_created_at'] = pd.to_datetime(df_events['event_created_at']).dt.tz_localize(None)

        print(f"Encontrados {len(df_events)} registros de convidados em um total de {len(all_events)} eventos.")
        return df_events

    except HttpError as error:
        print(f'Ocorreu um erro na API do Google Calendar: {error}')
        return pd.DataFrame()

def main():
    """Função principal para orquestrar o processo."""
    df_leads = get_supabase_leads()
    df_events = get_google_calendar_events()

    if df_leads.empty or df_events.empty:
        print("Não há dados suficientes para calcular a métrica. Encerrando.")
        return

    # --- Lógica de Negócio ---
    # 1. Unir os dados de leads com os de eventos usando o email como chave
    merged_df = pd.merge(df_leads, df_events, left_on='email', right_on='attendee_email', how='inner')

    # 2. Para cada lead, pode haver múltiplos eventos. Queremos o PRIMEIRO.
    # Ordenamos por data de criação do evento e removemos duplicatas de leads, mantendo a primeira ocorrência.
    merged_df = merged_df.sort_values(by='event_created_at', ascending=True)
    first_call_df = merged_df.drop_duplicates(subset='email', keep='first')

    if first_call_df.empty:
        print("Nenhum lead encontrado com um evento de call agendado.")
        return

    # 3. Calcular a diferença de tempo (Speed)
    first_call_df['speed'] = first_call_df['event_created_at'] - first_call_df['created_at']
    
    # Ignorar tempos negativos (caso o evento tenha sido criado antes do lead, o que pode ser um erro de dados)
    first_call_df = first_call_df[first_call_df['speed'] >= pd.Timedelta(0)]
    
    if first_call_df.empty:
        print("Nenhum lead com agendamento válido (evento criado após o lead) foi encontrado.")
        return

    # 4. Calcular a média
    average_speed = first_call_df['speed'].mean()
    
    # --- Apresentar o Resultado ---
    print("\n--- RESULTADO: Lead to Opportunity Speed ---")
    print(f"Baseado em {len(first_call_df)} leads que tiveram uma call agendada.")
    
    # Convertendo o resultado (Timedelta) para um formato mais legível
    total_seconds = average_speed.total_seconds()
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    
    print("\nTempo médio entre a criação do lead e o agendamento da primeira call:")
    print(f"--> {average_speed}")
    print(f"--> Ou, em formato mais amigável: {int(days)} dias, {int(hours)} horas e {int(minutes)} minutos.")


if __name__ == '__main__':
    main()