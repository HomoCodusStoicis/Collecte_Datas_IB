import os
from twilio.rest import Client 
# les credentials sont lues depuis les variables d'environnement TWILIO_ACCOUNT_SID et AUTH_TOKEN 
# TWILIO_ACCOUNT_SID='AC33df42051033b8da4eabdcb659eadcc3'
# TWILIO_AUTH_TOKEN='2d6eba7ad1525258893df90a31db15ef'

os.environ['TWILIO_ACCOUNT_SID']='AC33df42051033b8da4eabdcb659eadcc3'
os.environ['TWILIO_AUTH_TOKEN']='2d6eba7ad1525258893df90a31db15ef'

client = Client() 
# c'est le numéro de test de la sandbox WhatsApp
from_whatsapp_number='whatsapp:+14155238886' 
# remplacez ce numéro avec votre propre numéro WhatsApp
to_whatsapp_number='whatsapp:+33686784587' 
client.messages.create(body='COUCOU VALENTIN JE T AIME TRES FORT', from_=from_whatsapp_number,to=to_whatsapp_number)