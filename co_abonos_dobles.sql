-- Unión entre Zendesk y transaccional uso para sacar abonos dobles.

{{
  config(
    table_type='iceberg',
    format='parquet',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['trx_numero_de_cuenta','zendesk_ticket','year','month','day']
)
}}

WITH
co_zendesk_abonos_monto_inferior_fraude as (
    SELECT
    TRIM(fields_payload['estado_del_abono'])                                            as estado_del_abono,
    CASE WHEN TRIM(fields_payload['nombre_del_solicitante']) = ''
        THEN TRIM(requester_name)
            ELSE TRIM(fields_payload['nombre_del_solicitante']) END                     as nombre_del_solicitante,
    TRIM(fields_payload['tipo_de_documento'])                                           as tipo_de_documento,
    TRIM(fields_payload['numero_de_documento'])                                         as numero_de_documento,
    TRIM(fields_payload['valor_del_fraude'])                                            as valor_del_fraude,
    ticket_id                                                                           as ticket,
    TRIM(fields_payload['numero_de_cuenta_870'])                                        as numero_de_cuenta,
    TRIM(fields_payload['numero_de_cuenta_870_para_restituir_el_dinero_del_fraude'])    as numero_de_cuenta_restitucion_dinero_fraude,
    TRIM(fields_payload['genero'])                                                      as genero,
    CASE WHEN TRIM(submitter_role) = 'agent' THEN submitter_name ELSE '' END            as nombre_del_agente,
    TRIM(fields_payload['cantidad_de_transacciones_reportadas'])                        as cantidad_de_transacciones_reportadas,
    TRIM(fields_payload['productos_y_canales'])                                         as productos_y_canales,
    TRIM(fields_payload['modalidad_de_fraude'])                                         as modalidad_de_fraude,
    TRIM(fields_payload['submodalidad_del_fraude'])                                     as submodalidad_del_fraude,
    TRIM(fields_payload['biometria_consistente'])                                       as biometria_consistente,
    TRIM(fields_payload['celular_de_quien_se_comunica'])                                as numero_celular_de_quien_se_comunica,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_01'])           as tran_id_01,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_02'])           as tran_id_02,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_03'])           as tran_id_03,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_04'])           as tran_id_04,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_05'])           as tran_id_05,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_06'])           as tran_id_06,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_07'])           as tran_id_07,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_08'])           as tran_id_08,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_09'])           as tran_id_09,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_10'])           as tran_id_10,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_11'])           as tran_id_11,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_12'])           as tran_id_12,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_13'])           as tran_id_13,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_14'])           as tran_id_14,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_15'])           as tran_id_15,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_16'])           as tran_id_16,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_17'])           as tran_id_17,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_18'])           as tran_id_18,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_19'])           as tran_id_19,
    TRIM(fields_payload['referencia_del_movimiento_vista_360deg_tran_id_20'])           as tran_id_20,
    TRIM(type)                                                                          as type,
    TRIM(fields_payload['tipo_de_solucion'])                                            as tipo_de_solucion,
    CASE WHEN TRIM(fields_payload['dictamen_del_fraude']) = 'descartado' THEN 'Favorable para el cliente con abono de dinero' ELSE TRIM(fields_payload['dictamen_del_fraude']) END as dictamen_del_fraude
    FROM {{source('servicio_raw','co_zendesk_flatten_tickets')}}
    WHERE   true
            AND ticket_form_label = 'gestion_de_fraude'
            AND brand_id = 1088748 --Colombia
            AND status IN ('hold', 'open', 'pending')
            AND type = 'incident'
            AND group_id IN (13850956415501)
            AND TRIM(fields_payload['modalidad_de_fraude']) in ('vulneración_de_cuentas_modalidad', 'compras_no_reconocidas_con_tarjeta_débito_modalidad')
            AND TRIM(fields_payload['submodalidad_del_fraude']) in ('transacción_no_reconocida_favorable_en_primer_contacto_gestión_fraude', 'telemercadeo_gestión_fraude')
),
base_trx as (
    select
        case when ft.otros_nombres = ' ' then ft.primer_nombre || ' ' || ft.primer_apellido || ' ' || ft.segundo_apellido
            else ft.primer_nombre || ' ' || ft.otros_nombres || ' ' || ft.primer_apellido || ' ' || ft.segundo_apellido  end as trx_nombre_completo
        , ft.numero_producto                                     as trx_numero_de_cuenta
        , ft.numero_identificacion                               as trx_numero_de_identificacion
        , date(ft.fecha_transaccion)                             as trx_fecha_de_transaccion
        , ft.hora_transaccion                                    as trx_hora_de_transaccion
        , ft.valor_transaccion                                   as trx_valor_de_transaccion
        , trim(ft.naturaleza_transaccion)                        as trx_naturaleza_de_transaccion
        , trim(ft.codigo_concepto)                               as trx_codigo_de_concepto
        , trim(ft.concepto_desc)                                 as trx_desc_concepto
        , trim(ft.tran_id)                                       as trx_tran_id_reclamado

        , BZ.estado_del_abono                                    as zendesk_estado_del_abono
        , BZ.nombre_del_solicitante                              as zendesk_nombre_del_solicitante
        , BZ.valor_del_fraude                                    as zendesk_valor_del_fraude
        , BZ.ticket                                              as zendesk_ticket
        , BZ.numero_de_cuenta                                    as zendesk_numero_de_cuenta
        , BZ.numero_de_cuenta_restitucion_dinero_fraude          as zendesk_numero_de_cuenta_restitucion_dinero_fraude
        , BZ.nombre_del_agente                                   as zendesk_nombre_del_agente
        , BZ.cantidad_de_transacciones_reportadas                as zendesk_cantidad_de_transacciones_reportadas
        , BZ.modalidad_de_fraude                                 as zendesk_modalidad_de_fraude
        , BZ.submodalidad_del_fraude                             as zendesk_submodalidad_del_fraude
        , BZ.biometria_consistente                               as zendesk_biometria_consistente
        , BZ.tran_id_01                                          as zendesk_tran_id_01
        , BZ.tran_id_02                                          as zendesk_tran_id_02
        , BZ.tran_id_03                                          as zendesk_tran_id_03
        , BZ.tran_id_04                                          as zendesk_tran_id_04
        , BZ.tran_id_05                                          as zendesk_tran_id_05
        , BZ.tran_id_06                                          as zendesk_tran_id_06
        , BZ.tran_id_07                                          as zendesk_tran_id_07
        , BZ.tran_id_08                                          as zendesk_tran_id_08
        , BZ.tran_id_09                                          as zendesk_tran_id_09
        , BZ.tran_id_10                                          as zendesk_tran_id_10
        , BZ.tran_id_11                                          as zendesk_tran_id_11
        , BZ.tran_id_12                                          as zendesk_tran_id_12
        , BZ.tran_id_13                                          as zendesk_tran_id_13
        , BZ.tran_id_14                                          as zendesk_tran_id_14
        , BZ.tran_id_15                                          as zendesk_tran_id_15
        , BZ.tran_id_16                                          as zendesk_tran_id_16
        , BZ.tran_id_17                                          as zendesk_tran_id_17
        , BZ.tran_id_18                                          as zendesk_tran_id_18
        , BZ.tran_id_19                                          as zendesk_tran_id_19
        , BZ.tran_id_20                                          as zendesk_tran_id_20
        , BZ.type                                                as zendesk_type
        , BZ.tipo_de_solucion                                    as zendesk_tipo_de_solucion
        , BZ.dictamen_del_fraude                                 as zendesk_dictamen_del_fraude
        , date_format(current_date, '%Y')                        as year
        , date_format(current_date, '%m')                        as month
        , date_format(current_date, '%d')                        as day

        , RANK() OVER (PARTITION BY ft.numero_identificacion ORDER BY ft.fecha_transaccion DESC) AS ranking
    from {{source('productos_raw','transaccional_uso')}} ft
    inner join co_zendesk_abonos_monto_inferior_fraude BZ on ft.numero_identificacion = BZ.numero_de_documento
    where cast(ft.year as bigint) >= year(current_date) - 1
    AND ft.codigo_concepto in ('A017', 'A105', 'A079', 'A082', 'A078')
    -- AND ft.numero_identificacion in ('1026566936')
)
select *, date_diff('day', date(trx_fecha_de_transaccion), now()) AS dias_transcurridos from base_trx
where true
and ranking = 1
and date_diff('day', date(trx_fecha_de_transaccion), now()) <= 365;