import os
import json
from base64 import b64encode
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import storage
from pyspark.sql import SparkSession

def load_config():
    with open('config.json') as f:
        return json.load(f)


def count_files_in_bucket(bucket_name, bucket_directory):
    # Obtener lista de blobs (archivos) en el bucket y ruta especificados
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=bucket_directory)

    # Contar la cantidad de blobs (archivos)
    numero_de_archivos = sum(1 for _ in blobs)
    return numero_de_archivos


def send_email(email_to, email_subject, email_body, email_sender, google_credencial):
    # Configurar servicio de Gmail
    scopes = ['https://www.googleapis.com/auth/gmail.send']
    credentials = service_account.Credentials.from_service_account_file(google_credencial, scopes=scopes)
    service = build('gmail', 'v1', credentials=credentials)

    # Crear mensaje
    message = f'From: {email_sender}\nTo: {email_to}\nSubject: {email_subject}\n\n{email_body}'
    message_bytes = message.encode('utf-8')
    message_b64 = b64encode(message_bytes).decode('utf-8')

    # Enviar mensaje
    send_message = service.users().messages().send(userId="me", body={'raw': message_b64}).execute()
    print(F'Email enviado con éxito a {email_to} con ID: {send_message["id"]}')


def spark_main(spark):
    config = load_config()
    numero_de_archivos = count_files_in_bucket(config['bucket_name'], config['bucket_directory'])

    # Enviar email si la cantidad de archivos supera el límite
    if numero_de_archivos > config['limite_archivos']:
        try:
            send_email(config['email_to'], config['email_subject'], config['email_body'], 
                       config['email_sender'], config['google_credencial'])
        except HttpError as error:
            print(F'Error al enviar el mensaje: {error}')
    else:
        print(F'La cantidad de archivos ({numero_de_archivos}) no supera el límite especificado ({config["limite_archivos"]}).')


if __name__ == '__main__':
    spark = SparkSession.builder.appName("example_app").getOrCreate()
    spark_main(spark)
    spark.stop()
