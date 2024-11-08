from openai import OpenAI
import time
from pygame import mixer
import os

class Assistant:
    def __init__(self, api_key, assistant_id, thread_id):
        self.client = OpenAI(api_key=api_key, default_headers={"OpenAI-Beta": "assistants=v2"})
        self.assistant_id = assistant_id
        self.thread_id = thread_id
        self.assistant = self.client.beta.assistants.retrieve(assistant_id)
        self.thread = self.client.beta.threads.retrieve(thread_id)
        #mixer.init()

    def ask_question_memory(self, question):
        """Envía una pregunta al asistente y espera la respuesta con memoria de contexto."""
        try:
            self.client.beta.threads.messages.create(self.thread.id, role="user", content=question)
            run = self.client.beta.threads.runs.create(thread_id=self.thread.id, assistant_id=self.assistant.id)
            
            # Polling para esperar a que se complete el procesamiento
            while (run_status := self.client.beta.threads.runs.retrieve(thread_id=self.thread.id, run_id=run.id)).status != 'completed':
                if run_status.status == 'failed':
                    return "The run failed."
                time.sleep(1)
            
            # Obtener los mensajes más recientes
            messages = self.client.beta.threads.messages.list(thread_id=self.thread.id)
            return messages.data[0].content[0].text.value
        
        except Exception as e:
            return f"Error al consultar al asistente: {str(e)}"
    
    def generate_tts(self, sentence, speech_file_path="speech.mp3"):
        """Genera el archivo de TTS a partir de una oración."""
        try:
            response = self.client.audio.speech.create(model="tts-1", voice="echo", input=sentence)
            response.stream_to_file(speech_file_path)
            return str(speech_file_path)
        except Exception as e:
            return f"Error al generar TTS: {str(e)}"
    
    def play_sound(self, file_path):
        """Reproduce un archivo de sonido."""
        try:
            mixer.music.load(file_path)
            mixer.music.play()
            while mixer.music.get_busy():
                time.sleep(1)
            mixer.music.unload()
            os.remove(file_path)
            return "done"
        except Exception as e:
            return f"Error al reproducir sonido: {str(e)}"
    
    def TTS(self, text):
        """Convierte texto en voz y lo reproduce."""
        try:
            speech_file_path = self.generate_tts(text)
            if "Error" not in speech_file_path:
                return self.play_sound(speech_file_path)
            return speech_file_path
        except Exception as e:
            return f"Error en TTS: {str(e)}"
