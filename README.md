# Censo RAIA

Descarga del censo publico de animales del Registro Andaluz de Identificacion Animal. El dataset se obtiene desde la API publica de RAIA y se guarda en NDJSON para procesado incremental.

## Recursos

- OpenAPI: https://www.juntadeandalucia.es/ssdigitales/festa/download-pro/raia-open-api.json
- Endpoint: https://api.raia.es/api/censoPublico

## Descarga

```bash
make setup
make data
```

## Estructura de datos

```
data/
  raw/
    raia_censo.ndjson
```

Cada linea es un objeto JSON con los datos de un animal.

## Campos del dataset

- `identificador` Identificador anonimo del animal. El OpenAPI indica `identidicador` pero los datos reales usan `identificador`.
- `especie` Especie del animal.
- `raza` Raza principal.
- `raza2` Raza secundaria si existe.
- `capa` Capa o color.
- `sexo` Sexo del animal.
- `tamano` Rango de peso en texto.
- `fechaNacimiento` Fecha de nacimiento en formato ISO.
- `fechaIdentificacion` Fecha de identificacion en formato ISO o null.
- `municipio` Municipio de residencia.
- `provincia` Provincia.
- `codigoPostal` Codigo postal.
- `codigoIneLocalidad` Codigo INE de localidad.
- `codigoIneProvincia` Codigo INE de provincia.
- `baja` Estado de baja. En el dataset observado aparece como string.

## Filtros disponibles en la API

La API acepta parametros de consulta para filtrar el censo antes de descargar.

- `especie`
- `capa`
- `raza`
- `raza2`
- `sexo`
- `codigoPostal`
- `codigoIneLocalidad`
- `codigoIneProvincia`
- `baja`

Por defecto solo se devuelven animales no dados de baja.

## Licencia

MIT.
