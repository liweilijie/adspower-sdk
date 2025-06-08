# Makefile

SERVER = 192.168.1.251
SERVER_DIR = /home/sp/py/code/adspower-sdk
SSH_PORT = 22

upload:
	@echo "Deploying to server $(SERVER) at $(SERVER_DIR)..."
	@rsync -av -e "ssh -p $(SSH_PORT)" --exclude-from='exclude.conf' . sp@$(SERVER):$(SERVER_DIR)

clean:
	@echo "🧹 Cleaning build directories..."
	@rm -rf build/ dist/ *.egg-info
	@echo "✅ Clean completed"

build: clean
	@echo "🔨 Building package..."
	@if [ ! -f "setup.py" ] && [ ! -f "pyproject.toml" ]; then \
		echo "❌ Error: setup.py or pyproject.toml not found"; \
		exit 1; \
	fi
	@python -m pip install --upgrade pip build twine
	@python -m build
	@echo "✅ Build completed"

publish: build
	@echo "📦 Publishing to PyPI..."
	@if [ ! -f "~/.pypirc" ]; then \
		echo "⚠️  Warning: ~/.pypirc not found. Make sure you have PyPI credentials set up."; \
	fi
	@echo "🚀 Uploading to PyPI..."
	@twine upload dist/*
	@echo "🚀 Uploading to TestPyPI..."
	@twine upload -r testpypi dist/*
	@echo "✅ Package published successfully!"
	@echo "\n📝 To install the package, run:"
	@echo "pip install --upgrade adspower-sdk"
	@echo "\n📝 To install from TestPyPI, run:"
	@echo "pip install --index-url https://test.pypi.org/simple/ --upgrade adspower-sdk"