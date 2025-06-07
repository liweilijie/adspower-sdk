# Makefile

SERVER = 192.168.1.251
SERVER_DIR = /home/sp/py/code/adspower-sdk
SSH_PORT = 22

upload:
	@echo "Deploying to server $(SERVER) at $(SERVER_DIR)..."
	@rsync -av -e "ssh -p $(SSH_PORT)" --exclude-from='exclude.conf' . sp@$(SERVER):$(SERVER_DIR)