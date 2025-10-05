#!/bin/bash

# Script para testar a API Delta Sharing

# Configurar Java 17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

echo "🧪 Testando Delta Sharing API..."
echo ""

# Configurar token
TOKEN="test"

# Verificar se o servidor está rodando
echo "1️⃣ Verificando se o servidor está rodando..."
if curl -s http://localhost:8080/ > /dev/null 2>&1; then
    echo "✅ Servidor está rodando"
else
    echo "❌ Servidor não está rodando. Execute ./run.sh primeiro"
    exit 1
fi

echo ""
echo "2️⃣ Testando endpoint /delta-sharing/shares..."
RESPONSE=$(curl -s -w "\n%{http_code}" -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares)

HTTP_CODE=$(echo "$RESPONSE" | tail -n 1)
BODY=$(echo "$RESPONSE" | head -n -1)

echo "HTTP Status: $HTTP_CODE"
echo "Response:"
echo "$BODY" | jq . 2>/dev/null || echo "$BODY"

if [ "$HTTP_CODE" = "200" ]; then
    echo "✅ Teste passou!"
else
    echo "❌ Teste falhou! Esperado 200, recebido $HTTP_CODE"
fi

echo ""
echo "3️⃣ Testando endpoint /delta-sharing/shares/demo-share/schemas..."
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/demo-share/schemas | jq .

echo ""
echo "4️⃣ Testando endpoint /delta-sharing/shares/demo-share/schemas/default/tables..."
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/demo-share/schemas/default/tables | jq .

echo ""
echo "✅ Testes concluídos!"
