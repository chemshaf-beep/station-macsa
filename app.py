# -*- coding: utf-8 -*-
# ==============================================================================
# 🦅 PROXY RELAIS HTTP HAUTE PERFORMANCE VERCEL (0% CALCUL / 100% TRANSMISSION)
# # Fichier : app.py -> PASSERELLE DE DÉSENCLAVEMENT DE PORT SBE POUR PYTHONANYWHERE
# ==============================================================================
import os
import requests
from flask import Flask, Response, jsonify

app = Flask(__name__)

# Désactivation des avertissements de sécurité SSL liés aux infrastructures Falcon
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Endpoint d'autorité officielle de la Bourse de Tunis (Port 8443 accessible depuis Vercel)
URL_API_FALCON = "https://online.macsa.com.tn:8443/marketApi/sbe/market-report/11,12,13,32,33,99"

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def relayer_flux_SBE_brut(path):
    global URL_API_FALCON
    try:
        # Allocation d'une session de transport rapide
        session_relais = requests.Session()
        session_relais.headers.update({"User-Agent": "Mozilla/5.0 MAC-SA-PROXY-NODE"})
        
        # 📡 ASPIRATION ET TÉLÉPORTATION SYNCHRONE DU FLUX POINT-VIRGULE BRUT
        reponse_falcon = session_relais.get(URL_API_FALCON, verify=False, timeout=4.0)
        
        if reponse_falcon.status_code == 200:
            # Captation stricte de l'encodage d'origine de Place (latin1 / OLE)
            contenu_brut = reponse_falcon.text
            
            # Retransmission instantanée sans aucune modification vers le client appelant
            return Response(
                contenu_brut, 
                status=200, 
                mimetype="text/plain", 
                headers={"Content-Type": "text/plain; charset=latin1"}
            )
        else:
            return jsonify({"erreur": f"API Falcon injoignable, code HTTP: {reponse_falcon.status_code}"}), reponse_falcon.status_code
            
    except Exception as e_proxy:
        return jsonify({"erreur": f"Échec critique de la passerelle de téléportation : {str(e_proxy)}"}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)



