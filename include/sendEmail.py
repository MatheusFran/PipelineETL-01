import yagmail
from dotenv import load_dotenv
import os
import json

load_dotenv()

# Recupera as variáveis
DESTINATARIO = os.getenv("DESTINATARIO")
EMAIL = os.getenv("EMAIL")
SENHA = os.getenv("SENHA_EMAIL")

def send_email(**kwargs):
    data = kwargs['data']
    dados = data.xcom_pull(task_id='select_data')
    try:
        corpo_email = "Dados do Relatório Winners:\n\n"
        corpo_email += json.dumps(dados, indent=4, ensure_ascii=False)

        yag = yagmail.SMTP(EMAIL, SENHA)
        yag.send(
            to=DESTINATARIO,
            subject='Relatório de Dados - Winners',
            contents=corpo_email
        )
        return("E-mail enviado com sucesso!")

    except Exception as e:
        return(f"Erro ao enviar e-mail: {e}")
