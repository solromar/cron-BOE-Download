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
boe_api_sumario = boe_url + "/diario_boe/xml.php?id=BOE-S-"


# --------------------------  Función que descarga y valida el XML del sumario ----------------------------------------- #
def descargar_y_validar_xml(url):
    response = requests.get(url, timeout=240)
    if response.status_code == 200 and len(response.content) > 10:
        try:
            xmlSumario = ET.fromstring(response.content)
            if xmlSumario.tag == "error":
                log_info("AVISO: No existen boletines para la fecha.")
                return None
            return response.content
        except ET.ParseError:
            log_info("ERROR: El sumario XML no pudo ser procesado.")
            return None
    else:
        log_info(f"Error al descargar el XML del sumario de {url}. Código de estado: {response.status_code}.")
        return None
    
# --------------------------  Descarga los PDFs y los sube a Blob Storage ----------------------------------------- #
def descargar_y_subir_pdfs(pdfs, destino_fecha):
    for pdf in pdfs:
        fichero_pdf_blob_path = f"{destino_fecha}/pdfs/{os.path.basename(pdf.text)}"
        logging.info("Descargando y subiendo: %s --> %s", boe_url + pdf.text, fichero_pdf_blob_path)
        subir_documento_a_blob(boe_url + pdf.text, fichero_pdf_blob_path, content_type='application/pdf')  
    
# --------------------------  Sube a Azure Blob Storage el archico XML y PDFs ----------------------------------------- #
def subir_documento_a_blob(content, blob_path, content_type):
    try:
        blob_client = container_client.get_blob_client(blob_path)
        content_settings = ContentSettings(content_type=content_type)
        blob_client.upload_blob(content, overwrite=True, content_settings=content_settings)
        log_info(f"Documento subido exitosamente al BLOB STORAGE: {container_name}, con ContentType: {content_type}, Y LA RUTA ES: {blob_path}")
    except Exception as e:
        log_info(f"No se pudo subir el documento a {blob_path}")

          

# -------------------------- Función principal ejecutada por Azure Timer Trigger ------------------------------ #
def main(myTimer: func.TimerRequest) -> None:
    global logs
    logs.clear()  # Limpia los logs al inicio de la ejecución
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    if myTimer.past_due:
        logging.info("The timer is past due!")
    logging.info("Python timer trigger function ran at %s", utc_timestamp)

    # para testear una fecha especifica
    #fecha_fija = datetime.datetime(2024, 2, 25)
    #hoy = fecha_fija.strftime("%Y%m%d")

    # Obtiene la fecha actual para procesar el BOE de hoy
    hoy = datetime.datetime.now().strftime("%Y%m%d")
    fecha_Ymd = hoy
    logging.info("Fecha: %s" % fecha_Ymd)    

    # Construye la ruta de destino /dias/año/mes/dia/index.xml y lo sube a Blob Storage con ContentType como 'application/xml'.
    destino_fecha = f'dias/{hoy[:4]}/{hoy[4:6]}/{hoy[6:]}'
    fichero_sumario_xml_url = boe_api_sumario + fecha_Ymd
    fichero_sumario_xml_blob = f"{destino_fecha}/index.xml"

    # Descargar y validar el XML del sumario
    xml_content = descargar_y_validar_xml(fichero_sumario_xml_url)
    if xml_content:
        # Si el XML es válido, proceder a subirlo
        subir_documento_a_blob(xml_content, fichero_sumario_xml_blob, 'application/xml')
        log_info("Sumario XML descargado y subido correctamente.")

        # Procesar el XML y subir los PDFs...
        try:
            xmlSumario = ET.fromstring(xml_content)
            pdfs = xmlSumario.findall(".//urlPdf")
            descargar_y_subir_pdfs(pdfs, destino_fecha)
        except ET.ParseError as e:
            log_info("ERROR: Sumario XML no pudo ser procesado")
    else:
        log_info("No se encontró un sumario XML válido para la fecha especificada.")

    # Ejemplo de cómo llamar a send_email y enviar logs por correo...
    #send_email(f"Resumen de la Ejecución de Azure Function Cron Borme de fecha: {fecha_Ymd}", "<br>".join(logs))

