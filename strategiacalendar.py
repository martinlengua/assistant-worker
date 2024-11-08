# import msal
# import requests

# # Configuración de la aplicación de Microsoft
# CLIENT_ID = "dcf706b1-ea23-47d3-b100-b1d1dfaf6fba"  # ID de la aplicación registrada en Microsoft
# AUTHORITY = "https://login.microsoftonline.com/common"
# SCOPES = ["Calendars.ReadWrite"]

# # Autenticación interactiva del usuario final para obtener token de acceso
# def get_access_token():
#     app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY)
#     result = app.acquire_token_interactive(scopes=SCOPES)
#     if "access_token" in result:
#         return result["access_token"]
#     else:
#         print("Error en autenticación:", result.get("error"), result.get("error_description"))
#         return None

# # Listar eventos en el calendario del usuario
# def list_events():
#     token = get_access_token()
#     if token:
#         headers = {"Authorization": f"Bearer {token}"}
#         response = requests.get("https://graph.microsoft.com/v1.0/me/calendar/events", headers=headers)
#         if response.status_code == 200:
#             events = response.json().get("value", [])
#             for event in events:
#                 print(f"Evento: {event['subject']}, Inicio: {event['start']['dateTime']}, Fin: {event['end']['dateTime']}")
#         else:
#             print("Error al listar eventos:", response.status_code, response.text)

# # Crear un evento en el calendario del usuario
# def create_event(subject, content, start_time, end_time, attendees_emails=[]):
#     token = get_access_token()
#     if token:
#         headers = {
#             "Authorization": f"Bearer {token}",
#             "Content-Type": "application/json"
#         }
#         event = {
#             "subject": subject,
#             "body": {
#                 "contentType": "HTML",
#                 "content": content
#             },
#             "start": {
#                 "dateTime": start_time,
#                 "timeZone": "America/Lima"
#             },
#             "end": {
#                 "dateTime": end_time,
#                 "timeZone": "America/Lima"
#             },
#             "attendees": [{"emailAddress": {"address": email, "name": email.split('@')[0]}, "type": "required"} for email in attendees_emails]
#         }
        
#         response = requests.post("https://graph.microsoft.com/v1.0/me/calendar/events", headers=headers, json=event)
#         if response.status_code == 201:
#             print("Evento creado exitosamente:", response.json().get("webLink"))
#         else:
#             print("Error al crear evento:", response.status_code, response.text)

# # Ejemplo de uso
# print("Listando eventos:")
# list_events()

# print("\nCreando un nuevo evento:")
# create_event(
#     subject="Reunión de Proyecto",
#     content="Revisión del proyecto.",
#     start_time="2024-11-01T09:00:00",
#     end_time="2024-11-01T10:00:00",
#     attendees_emails=["invitado1@example.com", "invitado2@example.com"]
# )



from azure.identity import ClientSecretCredential
import requests
import os

# Configuración con variables de entorno (obtenidas desde el Secret de Kubernetes)
CLIENT_ID = os.environ["CLIENT_ID"]  # ID de la aplicación
CLIENT_SECRET = os.environ["CLIENT_SECRET"]  # Secreto de la aplicación
TENANT_ID = os.environ["TENANT_ID"]  # ID del tenant (directorio)
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"

# Autenticación con ClientSecretCredential
def get_access_token():
    credential = ClientSecretCredential(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        tenant_id=TENANT_ID
    )
    token = credential.get_token("https://graph.microsoft.com/.default")
    return token.token

# Reemplaza el correo electrónico del usuario para quien deseas crear el evento
USER_EMAIL = "martin@strategiaperutech.com"  # Reemplaza con el correo deseado

def list_events():
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{GRAPH_API_ENDPOINT}/users/{USER_EMAIL}/calendar/events", headers=headers)
    if response.status_code == 200:
        events = response.json().get("value", [])
        for event in events:
            print(f"Evento: {event['subject']}, Inicio: {event['start']['dateTime']}, Fin: {event['end']['dateTime']}")
    else:
        print("Error al listar eventos:", response.status_code, response.text)

def create_event(subject, content, start_time, end_time, attendees_emails=[]):
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    event = {
        "subject": subject,
        "body": {
            "contentType": "HTML",
            "content": content
        },
        "start": {
            "dateTime": start_time,
            "timeZone": "America/Lima"
        },
        "end": {
            "dateTime": end_time,
            "timeZone": "America/Lima"
        },
        "attendees": [{"emailAddress": {"address": email, "name": email.split('@')[0]}, "type": "required"} for email in attendees_emails]
    }
    
    response = requests.post(f"{GRAPH_API_ENDPOINT}/users/{USER_EMAIL}/calendar/events", headers=headers, json=event)
    if response.status_code == 201:
        print("Evento creado exitosamente:", response.json().get("webLink"))
    else:
        print("Error al crear evento:", response.status_code, response.text)


# Ejemplo de uso
print("Listando eventos:")
list_events()

print("\nCreando un nuevo evento:")
create_event(
    subject="Reunión de Proyecto",
    content="Revisión del proyecto.",
    start_time="2024-11-01T09:00:00",
    end_time="2024-11-01T10:00:00",
    attendees_emails=["invitado1@example.com", "invitado2@example.com"]
)
