# TBP Data Pipeline

> **Sistema de ingesta de datos financieros desde Polygon.io S3 a TimescaleDB**

![Status](https://img.shields.io/badge/Status-Implementation-orange)
![Progress](https://img.shields.io/badge/Progress-65%25-green)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Database](https://img.shields.io/badge/Database-TimescaleDB-purple)

Sistema de pipeline de datos financieros para TBP (Trading Bot Project). Descarga automática de datos históricos de mercado desde Polygon.io S3 y almacenamiento en TimescaleDB con validación de calidad y retry logic robusto.

## 🎯 MVP Scope

- **12 tickers**: 10 stocks + 2 ETFs del portfolio personal
- **Timeframe**: Solo datos diarios (1d)
- **Histórico**: 5 años (2020-2024)
- **Fuente**: Polygon.io S3 Flat Files (primaria) + API (fallback)
- **Base de datos**: TimescaleDB con hypertables

## ✅ Estado de Implementación (65% completo)

### Componentes Completados
- **✅ ConfigLoader**: Sistema configuración YAML con template substitution
- **✅ S3Client**: Cliente descarga completo con retry logic y validación
- **✅ Database Schema**: TimescaleDB schema con hypertables e índices
- **✅ Testing**: Framework testing con LocalStack y pytest

### En Progreso
- **🔄 Database Client**: Cliente modular TimescaleDB con connection pooling
- **📋 Main Orchestration**: Lógica principal coordinación pipeline

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL 15+ con TimescaleDB 2.15+
- Polygon.io S3 credentials

### Instalación
```bash
# Clonar repositorio
git clone https://github.com/patofp/tbp-data-pipeline
cd tbp-data-pipeline

# Crear virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\\Scripts\\activate  # Windows

# Instalar dependencias
pip install -r requirements.txt

# Configurar environment
cp .env.example .env
# Editar .env con tus credenciales

# Cargar variables de entorno
source scripts/set-env-vars.sh

# Ejecutar migraciones database
python scripts/run_migrations.py upgrade
```

### Variables de Entorno Requeridas
```bash
# S3 credentials
POLYGON_S3_ACCESS_KEY=your_access_key
POLYGON_S3_SECRET_KEY=your_secret_key

# Database credentials  
DB_HOST=192.168.1.11
DB_PORT=5432
DB_NAME=trading
DB_USER=your_user
DB_PASSWORD=your_password

# Opcional
POLYGON_API_KEY=your_api_key
LOG_LEVEL=INFO
ENVIRONMENT=development
```

## 🏗️ Arquitectura

### Flujo de Datos
```
Polygon.io S3 → S3Client → DataValidation → TimescaleDB
                   ↓
              FailedDownload Tracking → Retry Logic
```

### Componentes Principales
```
src/
├── config_loader.py       # ✅ Gestión configuración YAML
├── s3_client.py           # ✅ Cliente Polygon.io S3
└── database/              # 🔄 Cliente modular TimescaleDB
    ├── client.py          #     Coordinador principal
    ├── market_data.py     #     Operaciones market_data_raw
    ├── failed_download.py #     Tracking fallos
    └── data_quality.py    #     Métricas calidad
```

### Schema Database
```sql
-- Tabla principal datos mercado (hypertable)
market_data_raw (ticker, timestamp, timeframe, data_source, OHLC, volume, transactions)

-- Tracking fallos para retry
failed_downloads (ticker, date, error_type, attempts, resolved_at)

-- Métricas calidad datos
data_quality_metrics (ticker, date, quality_score, rejection_reasons)
```

## 📊 Configuración

### 12 Tickers Configurados
```yaml
# High Priority (Mega cap tech)
["AAPL", "MSFT", "NVDA", "META"]

# Medium Priority (Large cap diversified)  
["JPM", "V", "CVX", "LLY", "UNH", "WMT"]

# ETFs (Market analysis)
["SPY", "QQQ"]
```

### Template Substitution
Todos los secrets usan template substitution:
```yaml
s3_config:
  credentials:
    access_key: "${POLYGON_S3_ACCESS_KEY}"
    secret_key: "${POLYGON_S3_SECRET_KEY}"
    
database:
  connection:
    host: "${DB_HOST}"
    password: "${DB_PASSWORD}"
```

## 🧪 Testing

```bash
# Tests unitarios (rápidos, con mocks)
pytest -m unit

# Tests integración (requiere Docker para LocalStack)
pytest -m integration

# Suite completa con coverage
pytest --cov=src --cov-report=term-missing

# Test específico
pytest test/test_config_loader.py -v
```

### LocalStack S3 Testing
Los tests de S3Client usan LocalStack para testing aislado:
```python
@pytest.fixture
def localstack_s3_client():
    # Configuración LocalStack con datos Polygon.io reales
    yield s3_client
```

## 🔧 Uso

### Test Configuración
```bash
# Verificar carga configuración
python -c "from src.config_loader import ConfigLoader; cl = ConfigLoader(); print(f'Loaded {len(cl.get_all_tickers())} tickers')"

# Verificar conexión S3
python -c "from src.s3_client import PolygonS3Client; from src.config_loader import ConfigLoader; cl = ConfigLoader(); client = PolygonS3Client(cl.get_s3_config()); print('S3 Connected!')"

# Verificar database (cuando esté implementado)
python -c "from src.database import TimescaleDBClient; client = TimescaleDBClient(); print(f'DB Connected: {client.test_connection()}')"
```

### Download Ejemplo (implementación futura)
```bash
# Download single ticker
python scripts/download_historical.py --ticker AAPL --start 2024-01-01 --end 2024-12-31

# Download all tickers
python scripts/download_historical.py --all --start 2024-01-01 --end 2024-12-31

# Dry run
python scripts/download_historical.py --ticker AAPL --dry-run
```

## 📈 Performance

### Métricas Actuales
- **ConfigLoader**: <50ms carga configuración
- **S3Client**: ~100 rows/segundo parsing CSV
- **Memory**: ~150KB por batch diario
- **Database**: Target >1K rows/segundo (en implementación)

### Targets MVP
- **Data Completeness**: >95% coverage
- **Daily Processing**: <15 minutos para 12 tickers
- **Storage**: ~15K records (5 años × 12 tickers × 252 días trading)
- **Memory Usage**: <1GB durante procesamiento

## 🛡️ Calidad de Datos

### Validación OHLC
```yaml
validation_rules:
  - high >= low
  - high >= open, close  
  - low <= open, close
  - volume >= 0
  - prices > 0
```

### Estrategia NaN
- **OHLC**: Rechazo registro completo
- **Volume**: Convertir a 0
- **Transactions**: Mantener NULL

### Error Handling
- Retry automático con exponential backoff
- Rate limiting detection
- FailedDownload tracking para Airflow
- Categorización errores (network, parsing, quality)

## 🔗 Documentación

### Arquitectura y Decisiones
- **[System Overview](docs/architecture/system-overview.md)**: Arquitectura completa actual
- **[Data Pipeline Decisions](docs/architecture/data-pipeline-decisions.md)**: Decisiones técnicas tomadas
- **[Progress Tracker](tbp-data-pipeline-tracker.md)**: Estado desarrollo actual

### Setup y Troubleshooting
- **[Database Setup](docs/database-setup.md)**: Configuración TimescaleDB
- **[CLAUDE.md](CLAUDE.md)**: Context para LLMs
- **[PLAN.md](PLAN.md)**: Plan desarrollo detallado

## 📋 Roadmap

### Inmediato (Esta Semana)
- [ ] Completar Database Client modular
- [ ] Implementar MarketDataClient con COPY protocol
- [ ] Testing integración database

### Corto Plazo (Próxima Semana)
- [ ] Main orchestration logic (downloader.py)
- [ ] CLI execution script (download_historical.py)
- [ ] Data quality validation framework
- [ ] End-to-end integration testing

### Mediano Plazo (Siguientes Semanas)
- [ ] Airflow DAGs para execution diaria
- [ ] Monitoring y alerting básico
- [ ] CI/CD pipeline setup
- [ ] Performance optimization

### Largo Plazo
- [ ] Integración con tbp-feature-lab
- [ ] Support para minute-level data
- [ ] Real-time data streaming
- [ ] Multi-asset class support

## 🤝 Contributing

### Development Guidelines
1. **Configuration-driven**: Toda configuración en YAML
2. **Test-first**: Escribir tests antes que código
3. **Error handling**: Categorizar errores, implementar retry logic
4. **Logging**: Structured JSON logs con context
5. **Performance**: Target >1K rows/sec inserts, <1GB memory

### Code Standards
- Python 3.11 con type hints en todas partes
- Seguir patrones existentes (mirar s3_client.py como referencia)
- Documentar decisiones y lógica compleja
- Manejar weekends/holidays automáticamente
- Usar connection pool, nunca crear conexiones directas

### Testing Strategy
```bash
# Unit tests (fast, mocked) - EJECUTAR FRECUENTEMENTE
pytest -m unit

# Integration tests (Docker required) - EJECUTAR ANTES DE COMMITS
pytest -m integration

# Full test suite with coverage
pytest --cov=src --cov-report=term-missing
```

## ⚠️ Important Rules

1. **NUNCA** commit sin ejecutar tests
2. **NUNCA** hardcodear credentials o secrets
3. **NUNCA** crear database connections fuera del connection pool
4. **SIEMPRE** manejar missing data gracefully (weekends, holidays)
5. **SIEMPRE** validar datos antes de insertar en database

## 📊 Success Metrics

- **Data Completeness**: >95% coverage
- **Daily Processing**: <15 minutos para todos los tickers
- **Test Coverage**: >80%
- **Insert Performance**: >1K rows/segundo
- **Error Recovery**: Retry automático exitoso >90%

## 🆘 Troubleshooting

### Database Issues
```bash
# Conectar a test DB
psql -h localhost -p 5433 -U test_user -d test_db

# Ver logs
docker logs tbp-data-pipeline-postgres-test-1

# Verificar hypertables
SELECT * FROM timescaledb_information.hypertables;
```

### S3 Connection Issues
```bash
# Test S3 connectivity
aws s3 ls s3://flatfiles/us_stocks_sip/day_aggs_v1/2024/01/ --endpoint-url=https://files.polygon.io

# Verificar credentials
echo $POLYGON_S3_ACCESS_KEY
echo $POLYGON_S3_SECRET_KEY
```

### Configuration Issues
```bash
# Verificar template substitution
python -c "from src.config_loader import ConfigLoader; cl = ConfigLoader(); print(cl.get_s3_config())"

# Listar variables entorno
env | grep -E '(POLYGON|DB_)'
```

## 📝 License

Proprietary - Trading Bot Project

## 🔗 Related Projects

- **tbp-feature-lab**: Feature engineering module (siguiente)
- **tbp-ml-engine**: ML training y deployment
- **tbp-bot-strategies**: Trading strategies implementation
- **tbp-paper-trading**: Paper trading orchestrator

---

**Recuerda**: Este es financial data - precisión y confiabilidad son primordiales!

Para más detalles, ver documentación completa en `/docs/` o contactar al team.

**Repository**: https://github.com/patofp/tbp-data-pipeline  
**Documentation**: Obsidian vault en `/mnt/d/obsidian-vaults/patofp/tbp-docs/`  
**Status**: Implementation Phase (65% complete)  
**Last Updated**: January 2025
