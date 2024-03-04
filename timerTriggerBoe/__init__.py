from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContainerClient,
    ContentSettings,
)
from time import sleep
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import logging
import azure.functions as func
import os
import requests
import datetime
import xml.etree.ElementTree as ET

app = func.FunctionApp()

# Configuración de Azure Blob Storage
connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = "boe"
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

# ------------------- Funcion para acumular los logs y enviarlos por mail con el resultado de la ejecución del cron --------------- #
logs = []

def log_info(message):
    global logs
    print(message)
    logs.append(message)


def send_email(subject, body):
    sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
    email = Mail(
        from_email="soledad@smartescrow.es",
        to_emails="soledad@smartescrow.es",
        subject=subject,
        html_content=body,
    )
    response = sg.send(email)
    print(response.status_code, response.body, response.headers)


boe_url = "https://boe.es"
boe_api_sumario = boe_url + '/diario_boe/xml.php?id=BOE-S-'


# --------------------------  Función descarga PDFs y subida a Blob Storage  ----------------------------------------- #
# Intenta descargar los PDFs del BORME y subirlo a Azure Blob Storage con un ContentType específico (application/pdf y application/xml). 
# Si la descarga de algún pdf falla, reintentará hasta 5 veces, esperando un minuto entre intentos.
def subir_documento_a_blob(url, blob_path, content_type):
        try:
            response = requests.get(url, stream=True, timeout=240)
            if response.status_code == 200:
                blob_client = container_client.get_blob_client(blob_path)
                content_settings = ContentSettings(content_type=content_type)
                blob_client.upload_blob(response.content, overwrite=True, content_settings=content_settings)
                log_info(f"Documento subido exitosamente al BLOB STORAGE: {container_name}, con ContentType: {content_type}, Y LA RUTA ES: {blob_path}")
                 
            else:
                log_info(f"Error al descargar el documento de {url}. Código de estado: {response.status_code}. Reintentando...")
        except requests.RequestException as e:
            log_info(f"No se pudo descargar el documento {url}")

# -------------------------- Función principal ejecutada por Azure Timer Trigger ------------------------------ #
def main(myTimer: func.TimerRequest) -> None:
    global logs
    logs.clear()  # Limpia los logs al inicio de la ejecución
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    if myTimer.past_due:
        logging.info("The timer is past due!")
    logging.info("Python timer trigger function ran at %s", utc_timestamp)

    # para testear una fecha especifica
    fecha_fija = datetime.datetime(2024, 3, 2)
    hoy = fecha_fija.strftime("%Y%m%d")

    # Obtiene la fecha actual para procesar el BOE de hoy
    #hoy = datetime.datetime.now().strftime("%Y%m%d")
    fecha_Ymd = hoy
    logging.info("Fecha: %s" % fecha_Ymd)    

    # Construye la ruta de destino /dias/año/mes/dia/index.xml y lo sube a Blob Storage con ContentType como 'application/xml'.
    destino_fecha = f'dias/{hoy[:4]}/{hoy[4:6]}/{hoy[6:]}'
    fichero_sumario_xml = f"{destino_fecha}/index.xml"
    logging.info("Solicitando %s --> %s", boe_api_sumario + fecha_Ymd, fichero_sumario_xml)
    subir_documento_a_blob(boe_api_sumario + fecha_Ymd, fichero_sumario_xml, content_type='application/xml')

    # Verifica el tamaño del sumario XML subido y procesa su contenido si es válido.
    blob_client = container_client.get_blob_client(fichero_sumario_xml)
    properties = blob_client.get_blob_properties()
    tamano_sumario_xml = properties.size
    logging.info("Recibidos: %s bytes", tamano_sumario_xml)
    

    # Verifica si el sumario XML es menor de 10 bytes, indicando un posible error o archivo vacío
    if tamano_sumario_xml < 10:
        log_info("ERROR: Sumario XML erróneo o incompleto")
        return

    # Descarga el contenido del sumario XML, lo procesa para encontrar y subir los PDFs mencionados en él
    xml_content = blob_client.download_blob().readall()
    try:
        xmlSumario = ET.fromstring(xml_content)
        if xmlSumario.tag == "error":
            log_info("AVISO: No existen boletines para la fecha %s" % fecha_Ymd)

        else:
            pdfs = xmlSumario.findall(".//urlPdf")
            for pdf in pdfs:
                fichero_pdf_blob_path = f"{destino_fecha}/pdfs/{os.path.basename(pdf.text)}"
                logging.info("Solicitando %s --> %s", boe_url + pdf.text, fichero_pdf_blob_path)                
                # Sube cada PDF encontrado al Blob Storage con ContentType como 'application/pdf'
                subir_documento_a_blob(boe_url + pdf.text, fichero_pdf_blob_path, content_type='application/pdf')
    except ET.ParseError:
        log_info("ERROR: Sumario XML no pudo ser procesado")      

        
# Ejemplo de cómo llamar a subir_documento_a_blob y enviar logs por correo
    send_email(f"Resumen de la Ejecución de Azure Function Cron BOE de fecha: {fecha_Ymd}", "<br>".join(logs))
