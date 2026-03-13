-- =============================================================================
-- PIPELINE METHANE —  SQL
-- Compatible with: DuckDB / PostgreSQL / SQLite (small differences)
-- =============================================================================
--
-- QUÉ ES SQL Y CÓMO LEERLO:
-- SQL trabaja con TABLAS (equivalente a un data.frame en R).
-- Cada instrucción termina en punto y coma (;).
-- El orden de ejecución es de arriba a abajo, paso a paso.
-- Los comentarios empiezan con -- (una línea) o /* ... */ (bloque).
--
-- KEY CONCEPTS:
--   SELECT   → elegir columnas  (equivale a select() en dplyr)
--   FROM     → de qué tabla     (equivale al nombre del data.frame)
--   WHERE    → filtrar filas    (equivale a filter())
--   JOIN     → unir tablas      (equivale a left_join(), inner_join()...)
--   GROUP BY → agrupar          (equivale a group_by())
--   WITH    → tabla temporal dentro de la consulta (equivale a objeto intermedio en R)
-- =============================================================================


-- =============================================================================
-- PAST 1: CREAR LAS TABLAS BASE
-- =============================================================================
-- En R los datos se leen desde archivos. En SQL primero hay que definir
-- las tablas donde vivirán esos datos. Aquí se asume que ya fueron cargadas
-- ( example, con DuckDB puedes hacer:
--   CREATE TABLE  AS 
SELECT * FROM read_csv_autosniffer_raw(
    'C:/Users/Ester/Documents/relivestock/nuevos_metano/nuevos_metano_output/*.txt',
    filename = true,
    union_by_name = true
);

SELECT COUNT(*) FROM sniffer_raw;
SELECT * FROM sniffer_raw LIMIT 3;

-- Tabla con los datos crudos del sniffer (uno por medición)
-- Equivale a: b
SELECT * FROM read_csv_auto(
    'C:/Users/Ester/Documents/relivestock/nuevos_metano/CONTROLES/*.csv',
    delim = ';',
    filename = true,
    union_by_name = true,
    ignore_errors = true,
    strict_mode = false
);
SELECT COUNT(*) FROM control_raw;
SELECT * FROM control_raw LIMIT 3;
 -- If it doesn't exist, create it :
CREATE TABLE IF NOT EXISTS sniffer_raw (
    CIB          TEXT,
    date         TEXT,
    farm         TEXT,
    robot        INTEGER,
    epiweek      INTEGER,
    epiyear      INTEGER,
    fecha_snif   DATE,
    meanCH4      DOUBLE,
    meanCH4_5s   DOUBLE,
    meanCO2      DOUBLE,
    AUC_CH4      DOUBLE,
    Sum_of_PeaksCH4     DOUBLE,
    Sum_of_PeaksCH4_5s  DOUBLE,
    Sum_MaxPeak         DOUBLE,
    kgm          DOUBLE,
    grasa        DOUBLE,
    proteina     DOUBLE,
    peaks_per_minute    DOUBLE
);

-- Tabla con los datos del control lechero
-- Equivale a: control_nuevo <- leer_control(RUTA_CONTROL)
CREATE TABLE IF NOT EXISTS control_lechero (
    cib           TEXT,
    farm          TEXT,
    robot         INTEGER,
    numero        TEXT,
    numpar        INTEGER,
    leche         DOUBLE,
    grasa         DOUBLE,
    proteina      DOUBLE,
    lactosa       DOUBLE,
    PesoVivo      DOUBLE,
    kgm           DOUBLE,
    fecha_control DATE,
    fpar          DATE,
    fcub          DATE
);

-- Tabla del pedigrí
-- Equivale a: ped_conafe <- read.table(...)
SELECT * FROM read_csv_auto(
    'C:/Users/Ester/Documents/relivestock/nuevos_metano/pedigri/nuevo_pedigri_nuevosmetano.txt',
    delim = ' ',
    header = false,
    union_by_name = true,
    ignore_errors = true
);

SELECT COUNT(*) FROM pedigri_raw;
SELECT * FROM pedigri_raw LIMIT 3;

-- Tabla maestra final (donde se acumula todo)
-- Equivale a: base_datos_final.rds
CREATE TABLE IF NOT EXISTS base_datos_final (
    numero       TEXT,
    robot        INTEGER,
    farm         TEXT,
    epiweek      INTEGER,
    year         INTEGER,
    comparison_group TEXT,
    PesoVivo     DOUBLE,
    CIB          TEXT,
    count        INTEGER,
    sire         TEXT,
    dam          TEXT,
    edad_anio    INTEGER,
    birth_date   DATE,
    num_lact     TEXT,
    EL_90        TEXT,
    EL_60        TEXT,
    EL_60_250    TEXT,
    calv_month   INTEGER,
    obs_date_floor DATE,
    meanCH4_promedio_semana          DOUBLE,
    meanCH4_gd_control_promedio_semana DOUBLE,
    ratiomeanch4co2_promedio_semana  DOUBLE,
    meanCO2_promedio_semana          DOUBLE,
    AUC_CH4_promedio_semana          DOUBLE,
    Sum_of_PeaksCH4_promedio_semana  DOUBLE,
    leche_promedio_semana            DOUBLE,
    kgm_promedio_semana              DOUBLE,
    grasa_kgm_control_promedio_semana   DOUBLE,
    proteina_kgm_control_promedio_semana DOUBLE,
    ECM_control_promedio_semana      DOUBLE,
    week_lact_promedio_semana        DOUBLE,
    media        INTEGER
);


-- =============================================================================
-- PASO 2: IDENTIFICAR SEMANAS NUEVAS (no procesadas aún)
-- =============================================================================
-- En R esto se hace con anti_join(semanas_archivo, semanas_existentes).
-- En SQL el patrón equivalente es LEFT JOIN + WHERE IS NULL:
-- "dame las semanas del sniffer que NO están en la base final".
--
-- WITH crea una tabla temporal que solo existe dentro de esta consulta.
-- Es como crear un objeto intermedio en R sin necesidad de guardarlo.

WITH semanas_en_master AS (
    -- Combinaciones farm+robot+semana que YA están procesadas en la base
    SELECT DISTINCT farm, robot, epiweek, year AS epiyear
    FROM base_datos_final
),
semanas_sniffer AS (
    -- Combinaciones que vienen en los nuevos datos del sniffer
    -- Solo semanas con al menos 3 registros CIB no nulos (control de calidad)
    SELECT
        farm,
        robot,
        epiweek,
        epiyear,
        COUNT(*) FILTER (WHERE CIB IS NOT NULL) AS n_cib
    FROM sniffer_raw
    GROUP BY farm, robot, epiweek, epiyear
    HAVING COUNT(*) FILTER (WHERE CIB IS NOT NULL) >= 3
)
-- Semanas nuevas = están en sniffer pero NO en master
-- Equivale a: anti_join(semanas_archivo, semanas_existentes, by=c("farm","robot","epiweek","epiyear"))
SELECT s.*
FROM semanas_sniffer s
LEFT JOIN semanas_en_master m
    ON  s.farm    = m.farm
    AND s.robot   = m.robot
    AND s.epiweek = m.epiweek
    AND s.epiyear = m.epiyear
WHERE m.farm IS NULL;
-- LEFT JOIN + WHERE IS NULL es el patrón estándar en SQL para anti_join:
-- si m.farm es NULL significa que no encontró coincidencia → la fila es nueva


-- =============================================================================
-- PASO 3: UNIR SNIFFER CON CONTROL LECHERO (pasos 6 y 7 del R)
-- =============================================================================
-- INNER JOIN solo mantiene filas con coincidencia en ambas tablas.
-- Equivale a inner_join(control_nuevo, by=c("CIB"="cib"))
-- ABS() = abs(), DATEDIFF() = as.numeric(fecha1 - fecha2)
-- BETWEEN equivale a filter(x >= a & x <= b)

CREATE TABLE dataset_joint AS
WITH nuevas_semanas AS (
    -- Subquery con las semanas que NO están en master (mismo cálculo que paso 2)
    SELECT s.farm, s.robot, s.epiweek, s.epiyear
    FROM (
        SELECT farm, robot, epiweek, epiyear
        FROM sniffer_raw
        GROUP BY farm, robot, epiweek, epiyear
        HAVING COUNT(*) FILTER (WHERE CIB IS NOT NULL) >= 3
    ) s
    LEFT JOIN (
        SELECT DISTINCT farm, robot, epiweek, year AS epiyear
        FROM base_datos_final
    ) m ON s.farm=m.farm AND s.robot=m.robot
       AND s.epiweek=m.epiweek AND s.epiyear=m.epiyear
    WHERE m.farm IS NULL
),
sniffer_nuevo AS (
    -- Filtrar sniffer_raw para quedarnos solo con las semanas nuevas
    -- Equivale a: semi_join(datos, semanas_nuevas, ...)
    SELECT sr.*
    FROM sniffer_raw sr
    INNER JOIN nuevas_semanas ns
        ON  sr.farm    = ns.farm
        AND sr.robot   = ns.robot
        AND sr.epiweek = ns.epiweek
        AND sr.epiyear = ns.epiyear
)
SELECT
    n.*,
    c.numero,
    c.numpar,
    c.leche,
    c.grasa        AS grasa_control,
    c.proteina     AS proteina_control,
    c.lactosa,
    c.PesoVivo,
    c.fecha_control,
    c.fpar,
    c.fcub,
    ABS(DATEDIFF('day', n.fecha_snif, c.fecha_control)) AS dif_dias,
    DATEDIFF('day', c.fpar, n.fecha_snif)               AS diff_fpar
FROM sniffer_nuevo n
INNER JOIN control_lechero c ON n.CIB = c.cib
WHERE
    ABS(DATEDIFF('day', n.fecha_snif, c.fecha_control)) <= 40
    AND DATEDIFF('day', c.fpar, n.fecha_snif) BETWEEN 5 AND 365
    AND c.numpar <= 8;


-- =============================================================================
-- PASO 4: LIMPIAR CEROS Y RECODIFICAR VARIABLES PRODUCTIVAS (paso 8 del R)
-- =============================================================================
-- NULLIF(valor, 0) → devuelve NULL si el valor es 0, si no devuelve el valor.
--   Es el equivalente de na_if(., 0) en dplyr.
-- El operador || concatena texto (equivale a paste(..., sep="-") en R).

CREATE TABLE dataset_clean AS
SELECT
    *,
    NULLIF(kgm,      0)                                  AS kgm_clean,
    NULLIF(grasa,    0)                                  AS grasa_clean,
    NULLIF(proteina, 0)                                  AS proteina_clean,
    NULLIF(lactosa,  0)                                  AS lactosa_clean,
    NULLIF(meanCO2,  0)                                  AS meanCO2_clean,
    -- Calcular kg de grasa y proteína (de % a kg)
    NULLIF(grasa,   0) * NULLIF(kgm,   0) / 100.0       AS grasa_kgm_robot,
    NULLIF(proteina,0) * NULLIF(kgm,   0) / 100.0       AS proteina_kgm_robot,
    NULLIF(grasa,   0) * NULLIF(leche, 0) / 100.0       AS grasa_kgm_control,
    NULLIF(proteina,0) * NULLIF(leche, 0) / 100.0       AS proteina_kgm_control,
    -- Grupo de comparación: farm-robot-year-epiweek
    -- Equivale a: unite(col="comparison_group", c("farm","robot","epiyear","epiweek"), sep="-")
    farm || '-' || CAST(robot AS TEXT) || '-' ||
    CAST(epiyear AS TEXT) || '-' || CAST(epiweek AS TEXT) AS comparison_group,
    -- Poner a NULL variables de metano cuando CO2 < 2500
    -- Equivale a: across(vars_metano, ~ ifelse(meanCO2 < 2500, NA, .))
    CASE WHEN NULLIF(meanCO2,0) < 2500 THEN NULL ELSE meanCH4           END AS meanCH4_f,
    CASE WHEN NULLIF(meanCO2,0) < 2500 THEN NULL ELSE meanCH4_5s        END AS meanCH4_5s_f,
    CASE WHEN NULLIF(meanCO2,0) < 2500 THEN NULL ELSE AUC_CH4           END AS AUC_CH4_f,
    CASE WHEN NULLIF(meanCO2,0) < 2500 THEN NULL ELSE Sum_of_PeaksCH4   END AS Sum_of_PeaksCH4_f,
    CASE WHEN NULLIF(meanCO2,0) < 2500 THEN NULL ELSE Sum_of_PeaksCH4_5s END AS Sum_of_PeaksCH4_5s_f,
    CASE WHEN NULLIF(meanCO2,0) < 2500 THEN NULL ELSE Sum_MaxPeak        END AS Sum_MaxPeak_f,
    -- Reescalar para modelos bivariados
    AUC_CH4           / 100.0 AS AUC_CH4_modif,
    Sum_of_PeaksCH4   / 100.0 AS Sum_of_PeaksCH4_modif,
    Sum_of_PeaksCH4_5s / 10.0 AS Sum_of_PeaksCH4_5s_modif
FROM dataset_joint;


-- =============================================================================
-- PASO 5: UNIR CON PEDIGRÍ Y CALCULAR EDAD (paso 9 del R)
-- =============================================================================
-- LEFT JOIN mantiene todos los registros de la tabla izquierda aunque el
-- animal no esté en el pedigrí (campos del pedigrí quedan NULL en ese caso).
-- Equivale a: left_join(ped_conafe, by="numero")
-- DATE_TRUNC('week', fecha) = floor_date(fecha, unit="week") de lubridate
-- REPLACE(texto, 'A', 'B') = str_replace(texto, 'A', 'B') de stringr

CREATE TABLE dataset_ped AS
SELECT
    d.*,
    p.sire,
    p.dam,
    p.birth_date,
    p.country,
    DATEDIFF('day', p.birth_date, d.fecha_snif)                        AS edad_dias,
    ROUND(DATEDIFF('day', p.birth_date, d.fecha_snif) / 365.0, 0)     AS edad_anio,
    EXTRACT(MONTH FROM p.birth_date)                                   AS mes_nac,
    DATE_TRUNC('week', d.fecha_snif)                                   AS obs_date_floor,
    REPLACE(d.numero, 'ESPH', 'HOLESPF00')                            AS id_new,
    1                                                                  AS sniffer_type
FROM dataset_clean d
LEFT JOIN pedigri p ON d.numero = p.numero;


-- =============================================================================
-- PASO 6: CALCULAR RATIO CH4/CO2 Y DÍAS DE GESTACIÓN (paso 10 del R)
-- =============================================================================
-- NULLIF evita divisiones por cero.
-- GREATEST(0, valor) = ifelse(valor < 0, 0, valor): nunca devuelve negativo.
-- COALESCE(valor, 0) = ifelse(is.na(valor), 0, valor): sustituye NULL por 0.

CREATE TABLE dataset_ratio AS
SELECT
    *,
    CASE
        WHEN NULLIF(meanCO2_clean, 0) < 2500               THEN NULL
        WHEN ROUND(meanCH4_f / NULLIF(meanCO2_clean,0), 4) > 0.3 THEN NULL
        ELSE ROUND(meanCH4_f / NULLIF(meanCO2_clean, 0), 4)
    END AS ratiomeanch4co2,
    GREATEST(0, COALESCE(DATEDIFF('day', fecha_snif, fcub), 0))       AS dias_gestacion,
    CAST(PesoVivo AS DOUBLE)                                           AS PesoVivo_num
FROM dataset_ped;


-- =============================================================================
-- PASO 7: ELIMINAR OUTLIERS POR CUANTILES (paso 11 del R)
-- =============================================================================
-- PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY col) calcula el percentil 1%.
-- Es el equivalente de quantile(x, 0.01) dentro de un group_by() en R.
-- Se calcula por grupo farm+robot+epiyear, igual que en group_modify() del R.

CREATE TABLE dataset_sinoutliers AS
WITH percentiles AS (
    SELECT
        farm, robot, epiyear,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY meanCH4_f)         AS p01_meanCH4,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY meanCH4_f)         AS p99_meanCH4,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY meanCO2_clean)     AS p01_meanCO2,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY meanCO2_clean)     AS p99_meanCO2,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY AUC_CH4_f)         AS p01_AUC_CH4,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY AUC_CH4_f)         AS p99_AUC_CH4,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY Sum_of_PeaksCH4_f) AS p01_SumPeaks,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY Sum_of_PeaksCH4_f) AS p99_SumPeaks,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY Sum_MaxPeak_f)     AS p01_MaxPeak,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY Sum_MaxPeak_f)     AS p99_MaxPeak
    FROM dataset_ratio
    GROUP BY farm, robot, epiyear
)
SELECT
    r.*,
    -- Si el valor está fuera de [p01, p99] → NULL (es un outlier)
    -- Equivale a: data[[var]][outliers] <- NA en el bucle for de corregir_outliers_cuantiles()
    CASE WHEN r.meanCH4_f         BETWEEN p.p01_meanCH4  AND p.p99_meanCH4  THEN r.meanCH4_f         ELSE NULL END AS meanCH4_clean,
    CASE WHEN r.meanCO2_clean     BETWEEN p.p01_meanCO2  AND p.p99_meanCO2  THEN r.meanCO2_clean     ELSE NULL END AS meanCO2_clean2,
    CASE WHEN r.AUC_CH4_f         BETWEEN p.p01_AUC_CH4  AND p.p99_AUC_CH4  THEN r.AUC_CH4_f         ELSE NULL END AS AUC_CH4_clean,
    CASE WHEN r.Sum_of_PeaksCH4_f BETWEEN p.p01_SumPeaks AND p.p99_SumPeaks THEN r.Sum_of_PeaksCH4_f ELSE NULL END AS SumPeaksCH4_clean,
    CASE WHEN r.Sum_MaxPeak_f     BETWEEN p.p01_MaxPeak  AND p.p99_MaxPeak  THEN r.Sum_MaxPeak_f     ELSE NULL END AS SumMaxPeak_clean
FROM dataset_ratio r
INNER JOIN percentiles p
    ON r.farm=p.farm AND r.robot=p.robot AND r.epiyear=p.epiyear
WHERE r.meanCH4_f IS NOT NULL;   -- filter(!is.na(meanCH4))


-- =============================================================================
-- PASO 8: PRODUCCIÓN DE METANO EN GRAMOS/DÍA (paso 12 del R)
-- =============================================================================
-- AVG()    = mean(..., na.rm=TRUE)
-- STDDEV() = sd(..., na.rm=TRUE)
-- POWER(x, n) = x^n en R
-- NULLIF(sd, 0) evita dividir por cero cuando la desviación es 0.

CREATE TABLE dataset_metano AS
WITH ecm AS (
    SELECT
        *,
        leche * 0.25 + 12.2 * grasa_kgm_control + 7.7 * proteina_kgm_control AS ECM_control,
        kgm_clean * (0.25 + 0.122 * grasa_clean + 0.077 * proteina_clean)     AS ECM_robot,
        -- CO2 esperado según modelo de Madsen
        180 * 24 * 0.001 * (
            5.6    * POWER(PesoVivo_num, 0.75) +
            22     * (leche * 0.25 + 12.2 * grasa_kgm_control + 7.7 * proteina_kgm_control) +
            1.6e-5 * POWER(dias_gestacion, 3)
        ) AS co2_esp_madsen,
        (meanCO2_clean2 / 1e6) * 86400 AS co2_volumen
    FROM dataset_sinoutliers
),
stats_co2 AS (
    -- Media y SD de CO2 por grupo de comparación
    -- Equivale a: group_by(comparison_group) %>% summarise(media_co2_esp=mean(...), sd_co2_esp=sd(...))
    SELECT
        comparison_group,
        AVG(co2_esp_madsen)    AS media_co2_esp,
        STDDEV(co2_esp_madsen) AS sd_co2_esp,
        AVG(co2_volumen)       AS media_co2_volumen,
        STDDEV(co2_volumen)    AS sd_co2_volumen
    FROM ecm
    GROUP BY comparison_group
)
SELECT
    e.*,
    -- Corrección del CO2 (estandarización cruzada entre volumen real y esperado)
    -- Equivale a: co2_vol_corr = sd_co2_esp*(co2_vol - media_co2_vol)/sd_co2_vol + media_co2_esp
    s.sd_co2_esp * (e.co2_volumen - s.media_co2_volumen) / NULLIF(s.sd_co2_volumen, 0)
        + s.media_co2_esp AS co2_volumen_corregido,
    -- CH4 en gramos/día
    e.ratiomeanch4co2 * (
        s.sd_co2_esp * (e.co2_volumen - s.media_co2_volumen) / NULLIF(s.sd_co2_volumen, 0) + s.media_co2_esp
    ) * 0.657 AS meanCH4_gd_control,
    -- Fórmula de Madsen directa
    (0.714 * e.ratiomeanch4co2) * 180 * 24 * 0.001 * (
        5.6    * POWER(e.PesoVivo_num, 0.75) +
        22     * (e.leche * 0.25 + 12.2 * e.grasa_kgm_control + 7.7 * e.proteina_kgm_control) +
        1.6e-5 * POWER(e.dias_gestacion, 3)
    ) AS CH4_gramos_dia_madsen_control
FROM ecm e
LEFT JOIN stats_co2 s ON e.comparison_group = s.comparison_group;


-- =============================================================================
-- PASO 9: EFECTOS FIJOS — ESTADIO DE LACTACIÓN Y NÚMERO DE PARTO (paso 13 del R)
-- =============================================================================
-- CASE WHEN ... END es el equivalente exacto de case_when() de dplyr.
-- FLOOR(x / 7) = floor(diff_fpar / 7) en R.
-- EXTRACT(MONTH FROM fecha) = month(fecha) de lubridate.

CREATE TABLE dataset_efectos AS
SELECT
    *,
    CASE
        WHEN diff_fpar < 91  THEN '1'
        WHEN diff_fpar < 151 THEN '2'
        ELSE                      '3'
    END AS EL_90,
    CASE
        WHEN diff_fpar > 5   AND diff_fpar < 61  THEN '1'
        WHEN diff_fpar >= 60 AND diff_fpar < 151 THEN '2'
        WHEN diff_fpar >= 150 AND diff_fpar < 306 THEN '3'
        WHEN diff_fpar >= 305                     THEN '4'
    END AS EL_60,
    CASE
        WHEN diff_fpar > 5   AND diff_fpar < 71  THEN '1'
        WHEN diff_fpar > 70  AND diff_fpar < 151 THEN '2'
        WHEN diff_fpar > 150 AND diff_fpar < 251 THEN '3'
        WHEN diff_fpar > 250                     THEN '4'
    END AS EL_60_250,
    CASE
        WHEN numpar <= 1               THEN '1'
        WHEN numpar > 1 AND numpar <=2 THEN '2'
        WHEN numpar > 2 AND numpar <=8 THEN '3'
    END AS num_lact,
    FLOOR(diff_fpar / 7.0)       AS week_lact,
    EXTRACT(MONTH FROM fpar)     AS calv_month
FROM dataset_metano
WHERE meanCH4_gd_control <= 1200 OR meanCH4_gd_control IS NULL;


-- =============================================================================
-- PASO 10: MEDIAS SEMANALES Y FILTROS (paso 14 del R)
-- =============================================================================
-- AVG() = mean(..., na.rm=TRUE)
-- COUNT(*) = n()
-- HAVING filtra sobre grupos (equivale a filter() después de group_by())
-- COUNT(*) OVER (PARTITION BY col) = add_count(col) en dplyr:
--   calcula el conteo por grupo sin colapsar las filas, como una ventana.

CREATE TABLE medias_filtradas AS
WITH medias AS (
    -- Medias semanales por animal/robot/semana/año
    -- Equivale a: group_by(numero,robot,epiweek,epiyear) %>% summarise(across(is.numeric, mean))
    SELECT
        numero,
        robot,
        farm,
        epiweek,
        epiyear                                        AS year,
        comparison_group,
        PesoVivo_num                                   AS PesoVivo,
        CIB,
        id_new,
        COUNT(*)                                       AS count,
        sire, dam, edad_anio, birth_date,
        num_lact, EL_90, EL_60, EL_60_250,
        calv_month, obs_date_floor,
        AVG(meanCH4_clean)                             AS meanCH4_promedio_semana,
        AVG(meanCH4_gd_control)                        AS meanCH4_gd_control_promedio_semana,
        AVG(ratiomeanch4co2)                           AS ratiomeanch4co2_promedio_semana,
        AVG(meanCO2_clean2)                            AS meanCO2_promedio_semana,
        AVG(AUC_CH4_clean)                             AS AUC_CH4_promedio_semana,
        AVG(SumPeaksCH4_clean)                         AS Sum_of_PeaksCH4_promedio_semana,
        AVG(leche)                                     AS leche_promedio_semana,
        AVG(kgm_clean)                                 AS kgm_promedio_semana,
        AVG(grasa_kgm_control)                         AS grasa_kgm_control_promedio_semana,
        AVG(proteina_kgm_control)                      AS proteina_kgm_control_promedio_semana,
        AVG(ECM_control)                               AS ECM_control_promedio_semana,
        AVG(diff_fpar)                                 AS week_lact_promedio_semana,
        1                                              AS media
    FROM dataset_efectos
    WHERE meanCH4_clean IS NOT NULL
    GROUP BY
        numero, robot, farm, epiweek, epiyear, comparison_group,
        PesoVivo_num, CIB, id_new, sire, dam, edad_anio, birth_date,
        num_lact, EL_90, EL_60, EL_60_250, calv_month, obs_date_floor
    HAVING COUNT(*) BETWEEN 5 AND 35   -- filter(count >= 5 & count <= 35)
),
con_min_EL AS (
    -- Seleccionar la semana con menor estadio de lactación por animal y grupo
    -- Equivale a: group_by(comparison_group, numero) %>% slice_min(EL_60_250)
    -- ROW_NUMBER() numera las filas dentro de cada partición ordenadas por EL
    SELECT
        *,
        ROUND(week_lact_promedio_semana, 0) AS week_lact_round,
        ROW_NUMBER() OVER (
            PARTITION BY comparison_group, numero
            ORDER BY CAST(EL_60_250 AS INTEGER) ASC
        ) AS rn
    FROM medias
),
con_freq AS (
    -- Añadir frecuencia por grupo y por vaca en una sola pasada
    -- COUNT(*) OVER (PARTITION BY ...) = add_count() en dplyr:
    --   calcula el conteo por grupo SIN eliminar filas (función de ventana)
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY comparison_group) AS freq_grupo,
        COUNT(*) OVER (PARTITION BY numero)           AS freq_vaca
    FROM con_min_EL
    WHERE rn = 1   -- solo la fila con menor EL por grupo+animal
)
SELECT * FROM con_freq
WHERE freq_grupo >= 5    -- al menos 5 animales por grupo de comparación
  AND freq_vaca  >= 2;   -- al menos 2 semanas de medición por vaca


-- =============================================================================
-- PASO 11: INSERTAR EN LA BASE MAESTRA (paso 15 del R)
-- =============================================================================
-- INSERT INTO ... SELECT ... es el equivalente de bind_rows() en R.
-- NOT EXISTS evita insertar duplicados (equivale al distinct() + anti_join del R).

INSERT INTO base_datos_final
SELECT
    numero, robot, farm, epiweek, year, comparison_group,
    PesoVivo, CIB, count, sire, dam, edad_anio, birth_date,
    num_lact, EL_90, EL_60, EL_60_250, calv_month, obs_date_floor,
    meanCH4_promedio_semana, meanCH4_gd_control_promedio_semana,
    ratiomeanch4co2_promedio_semana, meanCO2_promedio_semana,
    AUC_CH4_promedio_semana, Sum_of_PeaksCH4_promedio_semana,
    leche_promedio_semana, kgm_promedio_semana,
    grasa_kgm_control_promedio_semana, proteina_kgm_control_promedio_semana,
    ECM_control_promedio_semana, week_lact_promedio_semana, media
FROM medias_filtradas mf
WHERE NOT EXISTS (
    -- No insertar si ya existe esa combinación animal+robot+semana+año
    -- Equivale al distinct() + anti_join del actualizar_master() en R
    SELECT 1 FROM base_datos_final bf
    WHERE bf.numero   = mf.numero
      AND bf.robot    = mf.robot
      AND bf.epiweek  = mf.epiweek
      AND bf.year     = mf.year
);


-- =============================================================================
-- CHEQUEOS FINALES (equivalente al bloque de verificación en R)
-- =============================================================================

-- Total de filas (equivale a nrow(base_final))
SELECT COUNT(*) AS total_filas FROM base_datos_final;

-- Número de animales únicos (equivale a n_distinct(base_final$numero))
SELECT COUNT(DISTINCT numero) AS n_animales FROM base_datos_final;

-- Distribución por granja (equivale a table(base_final$farm))
SELECT farm, COUNT(*) AS n
FROM base_datos_final
GROUP BY farm
ORDER BY farm;

-- Valores nulos por columna (equivale a colSums(is.na(base_final)))
-- COUNT(*) - COUNT(col) da el número de NULLs en cada columna
SELECT
    COUNT(*) - COUNT(numero)                           AS na_numero,
    COUNT(*) - COUNT(meanCH4_promedio_semana)          AS na_meanCH4,
    COUNT(*) - COUNT(meanCH4_gd_control_promedio_semana) AS na_meanCH4_gd,
    COUNT(*) - COUNT(PesoVivo)                         AS na_PesoVivo,
    COUNT(*) - COUNT(sire)                             AS na_sire,
    COUNT(*) - COUNT(birth_date)                       AS na_birth_date
FROM base_datos_final;
