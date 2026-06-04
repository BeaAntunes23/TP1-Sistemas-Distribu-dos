@echo off
echo === Setup do Servico de Analise ===
echo.
echo 1. Instalando dependencias Python...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo ERRO: falha ao instalar dependencias.
    pause & exit /b 1
)

echo.
echo 2. Gerando codigo gRPC a partir do proto...
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. analise.proto
if %errorlevel% neq 0 (
    echo ERRO: falha ao gerar codigo gRPC.
    pause & exit /b 1
)

echo.
echo === Setup concluido! ===
echo Para iniciar o servico: python server.py
echo.
pause
