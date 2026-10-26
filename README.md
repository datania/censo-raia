# Censo RAIA

Descarga del censo publico de animales del Registro Andaluz de Identificacion Animal. El dataset se obtiene desde la API publica de RAIA y se guarda en NDJSON para procesado incremental. Accede al dataset en [Hugging Face](https://huggingface.co/datasets/datania/censo-raia).

## Recursos

- Dataset: https://huggingface.co/datasets/datania/censo-raia
- OpenAPI: https://www.juntadeandalucia.es/ssdigitales/festa/download-pro/raia-open-api.json
- Endpoint: https://api.raia.es/api/censoPublico

## Descarga

```bash
make setup
make data
```

La descarga verifica TLS y solo sustituye el fichero anterior cuando ha recibido
un array JSON completo, no vacio y con registros de animales. Los fallos HTTP,
las respuestas invalidas y las descargas interrumpidas terminan con error y
conservan cualquier fichero anterior. No hay reintentos automaticos.

### Incidencia del proveedor (26 de octubre de 2026)

La [ejecucion de GitHub Actions 40397419484](https://github.com/datania/censo-raia/actions/runs/40397419484)
recibio HTTP 403 al solicitar el censo. El informe y la publicacion se omitieron.
Dos comprobaciones locales acotadas posteriores recibieron HTTP 500; la segunda
confirmo un mensaje generico `Server Error`, sin leer registros del censo.
La conexion local verifico correctamente el certificado TLS.

La [especificacion oficial](https://www.juntadeandalucia.es/ssdigitales/festa/download-pro/raia-open-api.json)
sigue indicando `GET https://api.raia.es/api/censoPublico`, sin filtros obligatorios
ni autenticacion documentada. No se ha identificado un endpoint sustituto ni una
credencial requerida. La diferencia entre 403 y 500 no permite determinar si hay
una restriccion de acceso o una averia del proveedor. Estas protecciones del
descargador **no restablecen el acceso**.

Para desbloquearlo, solicitar al contacto tecnico de la especificacion,
`informatica@colegiosveterinarios.es`, que confirme la disponibilidad del servicio,
el endpoint vigente y los requisitos de acceso automatizado desde GitHub Actions.
No se deben probar credenciales arbitrarias ni eludir controles de acceso.
Cuando RAIA confirme el acceso, validar con una unica ejecucion controlada.

### Pruebas

```bash
make test
```

Las pruebas usan respuestas sinteticas y no consultan RAIA ni publican datos.

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
