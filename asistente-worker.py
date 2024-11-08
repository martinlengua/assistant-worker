# worker.py
import os
import logging
from azure.servicebus import ServiceBusClient
from dotenv import load_dotenv
from twilio.rest import Client
from assist import Assistant
import time

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cargar variables de entorno
logging.info("Loading environment variables.")
load_dotenv()

# Configurar Twilio y OpenAI
account_sid = os.getenv("ACCOUNT_SID")
auth_token = os.getenv("AUTH_TOKEN")
twilio_number = os.getenv("TWILIO_NUMBER")
logging.info("Configuring Twilio client.")
client = Client(account_sid, auth_token)

# Configurar asistente de OpenAI
logging.info("Setting up OpenAI Assistant.")
assistant = Assistant(
    api_key=os.getenv("OPENAI_API_KEY"),
    assistant_id=os.getenv("ASSISTANT_ID"),
    thread_id=os.getenv("THREAD_ID")
)

# Configurar Azure Service Bus
connection_str = os.getenv("SERVICE_BUS_CONNECTION_STRING")
queue_name = os.getenv("SERVICE_BUS_QUEUE_NAME")
logging.info("Connecting to Azure Service Bus.")
service_bus_client = ServiceBusClient.from_connection_string(connection_str)

def process_conversation(conversation, from_number):
    """
    Envía la conversación acumulada a OpenAI y envía la respuesta final al usuario.
    """
    # Combina todos los mensajes de la conversación
    complete_conversation = " ".join(conversation)
    logging.info(f"Processing conversation for {from_number}: {complete_conversation}")

    # Envía la conversación completa a OpenAI
    logging.info("Sending conversation to OpenAI for a response.")
    reply = assistant.ask_question_memory(complete_conversation)

    # Enviar la respuesta a través de Twilio
    logging.info(f"Sending response to {from_number} via Twilio.")
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
    logging.info("Starting to monitor the Service Bus queue for messages.")

    with service_bus_client:
        receiver = service_bus_client.get_queue_receiver(queue_name=queue_name, max_wait_time=5)
        logging.info(f"Receiver connected to queue '{queue_name}'.")

        for msg in receiver:
            from_number, message_content = msg.body.split("|", 1)
            logging.info(f"Received message from {from_number}: {message_content}")

            # Agrega el mensaje a la conversación del remitente
            if from_number not in conversation_dict:
                logging.info(f"Creating a new conversation entry for {from_number}.")
                conversation_dict[from_number] = []

            conversation_dict[from_number].append(message_content)
            logging.info(f"Message added to conversation for {from_number}.")

            # Marca el mensaje como completado en Service Bus
            receiver.complete_message(msg)
            logging.info(f"Message from {from_number} marked as complete in Service Bus.")

            # Espera unos segundos para ver si hay más mensajes de este remitente
            logging.info(f"Waiting 3 seconds to check for additional messages from {from_number}.")
            time.sleep(3)

            # Si después de 3 segundos no hay nuevos mensajes, procesa la conversación
            if from_number in conversation_dict:
                logging.info(f"No more messages from {from_number}. Processing conversation.")
                process_conversation(conversation_dict[from_number], from_number)
                del conversation_dict[from_number]
                logging.info(f"Conversation with {from_number} has been processed and removed from dictionary.")

if __name__ == "__main__":
    logging.info("Starting the message processing application.")
    process_messages()
