# worker.py
import os
import logging
from azure.servicebus import ServiceBusClient
from dotenv import load_dotenv
from twilio.rest import Client
from assistant import Assistant
import time

# Cargar variables de entorno
load_dotenv()

# Configurar Twilio y OpenAI
account_sid = os.getenv("ACCOUNT_SID")
auth_token = os.getenv("AUTH_TOKEN")
twilio_number = os.getenv("TWILIO_NUMBER")
client = Client(account_sid, auth_token)

# Configurar asistente de OpenAI
assistant = Assistant(
    api_key=os.getenv("OPENAI_API_KEY"),
    assistant_id=os.getenv("ASSISTANT_ID"),
    thread_id=os.getenv("THREAD_ID")
)

connection_str = os.getenv("SERVICE_BUS_CONNECTION_STRING")
queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME")
service_bus_client = ServiceBusClient.from_connection_string(connection_str)

def process_conversation(conversation, from_number):
    """
    Envía la conversación acumulada a OpenAI y envía la respuesta final al usuario.
    """
    # Combina todos los mensajes de la conversación
    complete_conversation = " ".join(conversation)
    logging.info(f"Processing conversation for {from_number}: {complete_conversation}")

    # Envía la conversación completa a OpenAI
    reply = assistant.ask_question_memory(complete_conversation)

    # Enviar la respuesta a través de Twilio
    client.messages.create(
        body=reply,
        from_=twilio_number,
        to=from_number
    )
    logging.info(f"Response sent to {from_number}: {reply}")

def process_messages():
    """
    Monitorea la cola de Service Bus y procesa las conversaciones.
    """
    conversation_dict = {}

    with service_bus_client:
        receiver = service_bus_client.get_queue_receiver(queue_name=queue_name, max_wait_time=5)

        for msg in receiver:
            from_number, message_content = msg.body.split("|", 1)
            logging.info(f"Received message from {from_number}: {message_content}")

            # Agrega el mensaje a la conversación del remitente
            if from_number not in conversation_dict:
                conversation_dict[from_number] = []

            conversation_dict[from_number].append(message_content)

            # Marca el mensaje como completado en Service Bus
            receiver.complete_message(msg)

            # Espera unos segundos para ver si hay más mensajes de este remitente
            time.sleep(3)

            # Si después de 3 segundos no hay nuevos mensajes, procesa la conversación
            if from_number in conversation_dict:
                process_conversation(conversation_dict[from_number], from_number)
                del conversation_dict[from_number]

if __name__ == "__main__":
    process_messages()