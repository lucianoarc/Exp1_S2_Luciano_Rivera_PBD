CREATE USER PRY2206_SUAMTIVA1 IDENTIFIED BY "Pass123456789-";
GRANT CONNECT, RESOURCE TO PRY2206_SUAMTIVA1;
GRANT UNLIMITED TABLESPACE TO PRY2206_SUAMTIVA1;

/*SELECT * 
FROM DETALLE_DE_CLIENTES;

SELECT * 
FROM CLIENTE;

SELECT * 
FROM TIPO_CLIENTE;

SELECT * 
FROM TRAMO_EDAD;

SELECT * 
FROM  COMUNA;*/

DECLARE
    R_DETALLE DETALLE_DE_CLIENTES%ROWTYPE;
    V_MINID NUMBER;
    V_MAXID NUMBER;
    V_CORREO VARCHAR2(100);
    V_RUT NUMBER;
    V_CLIENTE VARCHAR2(150);
    V_EDAD NUMBER;
    V_PUNTAJE NUMBER := 0;
    V_CONTADOR NUMBER := 0;
    V_PERIOD VARCHAR2(7);
    V_RENTA NUMBER;
    V_COMUNA VARCHAR2(100);
    V_CATEGORIA VARCHAR2(50);
    V_TOTAL_CLIENTES NUMBER;
    
BEGIN
    -- Asignar el valor del periodo dinámicamente para marzo
    V_PERIOD := '03/2024';

   -- Truncar la tabla DETALLE_DE_CLIENTES antes de comenzar el proceso
    -- Esto permite que los datos se reinicien cada vez que se ejecuta el bloque PL/SQL
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_DE_CLIENTES';

   -- Obtener el ID mínimo y máximo de los clientes
    -- Esto nos ayudará a recorrer todos los clientes en el ciclo WHILE
    SELECT MIN(ID_CLI), MAX(ID_CLI)
    INTO V_MINID, V_MAXID
    FROM CLIENTE;

     -- Obtener el total de clientes para verificar el recorrido completo
    -- El contador se utiliza para confirmar que todos los clientes fueron procesados
    SELECT COUNT(*)
    INTO V_TOTAL_CLIENTES
    FROM CLIENTE
    WHERE MOD(ID_CLI, 5) = 0;
    
    -- Mostrar mensaje inicial indicando que se inició el procesamiento de los clientes
    DBMS_OUTPUT.PUT_LINE('PROCESANDO CLIENTES ...');

    -- Ciclo WHILE para recorrer los clientes
    WHILE V_MINID <= V_MAXID LOOP
        BEGIN
        -- Manejo de errores dentro de BEGIN...END
            BEGIN
                -- Obtener el RUT del cliente combinando el número de RUN y el dígito verificador
                -- Si el dígito verificador es 'K', se toma como 0
                SELECT NUMRUN_CLI * 10 + CASE
                    WHEN DVRUN_CLI = 'K' THEN 0
                    ELSE TO_NUMBER(DVRUN_CLI)
                END
                INTO V_RUT
                FROM CLIENTE
                WHERE ID_CLI = V_MINID;

                -- Obtener nombre completo del cliente
                SELECT INITCAP(PNOMBRE_CLI) || ' ' || INITCAP(APPATERNO_CLI) || ' ' || INITCAP(APMATERNO_CLI)
                INTO V_CLIENTE
                FROM CLIENTE
                WHERE ID_CLI = V_MINID;

               -- Calcular la edad del cliente utilizando la función MONTHS_BETWEEN
                -- La edad se calcula redondeando la diferencia de meses dividida por 12
                SELECT FLOOR(MONTHS_BETWEEN(SYSDATE, FECHA_NAC_CLI) / 12)
                INTO V_EDAD
                FROM CLIENTE
                WHERE ID_CLI = V_MINID;

                -- Obtener la renta, comuna y categoría del cliente mediante un JOIN con las tablas relacionadas
                SELECT C.RENTA, CM.NOMBRE_COMUNA, TC.NOMBRE_TIPO_CLI
                INTO V_RENTA, V_COMUNA, V_CATEGORIA
                FROM CLIENTE C
                JOIN COMUNA CM ON C.ID_COMUNA = CM.ID_COMUNA
                JOIN TIPO_CLIENTE TC ON C.ID_TIPO_CLI = TC.ID_TIPO_CLI
                WHERE C.ID_CLI = V_MINID;

                -- Aplicar reglas de negocio para calcular el puntaje del cliente
                -- Regla 1: Si la renta es superior a 700,000 y no vive en ciertas comunas, se calcula el 3% de la renta
                IF V_RENTA > 700000 AND V_COMUNA NOT IN ('La Reina', 'Las Condes', 'Vitacura') THEN
                    V_PUNTAJE := ROUND(V_RENTA * 0.03);
                ELSE
                -- Regla 2: Si el cliente es Internacional o VIP, se calcula 30 puntos por cada año de edad
                    IF V_CATEGORIA IN ('Internacional', 'Vip') THEN
                        V_PUNTAJE := V_EDAD * 30;
                    ELSE
                        -- Regla 3: Si el puntaje sigue siendo 0, obtener el porcentaje de TRAMO_EDAD y aplicarlo a la renta
                        BEGIN
                            SELECT PORCENTAJE
                            INTO V_PUNTAJE
                            FROM TRAMO_EDAD
                            WHERE V_EDAD BETWEEN TRAMO_INF AND TRAMO_SUP
                            ORDER BY TRAMO_SUP DESC
                            FETCH FIRST 1 ROW ONLY;
                            V_PUNTAJE := ROUND(V_RENTA * V_PUNTAJE / 100);
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                V_PUNTAJE := 0;
                        END;
                    END IF;
                END IF;

               -- Generar el correo electrónico simulado del cliente
                -- El formato del correo incluye el apellido paterno, edad, inicial del nombre y fecha de nacimiento
                SELECT LOWER(APPATERNO_CLI) || V_EDAD || '*' || SUBSTR(PNOMBRE_CLI, 1, 1) || TO_CHAR(FECHA_NAC_CLI, 'DD') || '3' || '@LogiCarg.cl'
                INTO V_CORREO
                FROM CLIENTE
                WHERE ID_CLI = V_MINID;


                -- Asignar valores al registro que se insertará en la tabla DETALLE_DE_CLIENTES
                R_DETALLE.IDC := V_MINID;
                R_DETALLE.RUT := V_RUT;
                R_DETALLE.CLIENTE := V_CLIENTE;
                R_DETALLE.EDAD := V_EDAD;
                R_DETALLE.PUNTAJE := V_PUNTAJE;
                R_DETALLE.CORREO_CORP := V_CORREO;
                R_DETALLE.PERIODO := V_PERIOD;

                -- Insertar los datos en la tabla DETALLE_DE_CLIENTES
                INSERT INTO DETALLE_DE_CLIENTES VALUES R_DETALLE;

                -- Incrementar el contador
                V_CONTADOR := V_CONTADOR + 1;

                 -- Incrementar el ID para pasar al siguiente cliente
                V_MINID := V_MINID + 5;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                                    -- Manejo de excepción si no se encuentra información para el cliente actual
                    DBMS_OUTPUT.PUT_LINE('No se encontró información para el cliente con ID: ' || V_MINID);
            END;
        END;
    END LOOP;

    -- Verificar si se recorrieron todos los clientes y confirmar la transacción
    IF V_CONTADOR = V_TOTAL_CLIENTES THEN
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Proceso Finalizado Exitosamente');
        DBMS_OUTPUT.PUT_LINE('Se Procesaron : ' || V_CONTADOR || ' CLIENTES');
    ELSE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: No se procesaron todos los clientes. Se realizó un rollback.');
    END IF;
END;
