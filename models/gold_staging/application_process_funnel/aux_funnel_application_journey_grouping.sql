{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
    country_code,
    synthetic_journey_name,
    is_synthetic::BOOLEAN AS is_synthetic,
    journey_homolog,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM VALUES
    ( 'CO', 'PROSPECT_CHECKPOINT_FINANCIA_CO'     , FALSE , 'CO-FINANCIA'    ),
    ( 'CO', 'PROSPECT_FINANCIA_CO'                , FALSE , 'CO-FINANCIA'    ),
    ( 'CO', 'PROSPECT_FINANCIA_SANTANDER_ADDI_CO' , TRUE  , 'CO-FINANCIA'    ),
    ( 'CO', 'PROSPECT_GATEWAY_FINANCIA_CO'        , FALSE , 'CO-FINANCIA'    ),
    ( 'CO', 'PREAPPROVAL_PAGO_CO'                 , FALSE , 'CO-PREAPPROVAL' ),
    ( 'CO', 'PREAPPROVAL_CHECKPOINT_PAGO_CO'      , FALSE , 'CO-PREAPPROVAL' ),
    ( 'CO', 'PROSPECT_FINANCIA_SANTANDER_CO'      , FALSE , 'CO-SANTANDER'   ),
    ( 'CO', 'PROSPECT_PAGO_CO'                    , FALSE , 'CO-PAGO'        ),
    ( 'CO', 'PROSPECT_PAGO_V2_CO'                 , FALSE , 'CO-PAGO'        ),
    ( 'CO', 'PROSPECT_CHECKPOINT_PAGO_CO'         , FALSE , 'CO-PAGO'        ),
    ( 'CO', 'PROSPECT_CHECKPOINT_PAGO_V2_CO'      , FALSE , 'CO-PAGO'        ),
    ( 'CO', 'CLIENT_PAGO_CO'                      , FALSE , 'CO-PAGO'        ),
    ( 'CO', 'CLIENT_PAGO_V2_CO'                   , FALSE , 'CO-PAGO'        ),
    ( 'CO', 'CLIENT_FINANCIA_CO'                  , FALSE , 'CO-PAGO'        ),
    ( 'CO', 'PROSPECT_GATEWAY_PAGO_CO'            , FALSE , 'CO-PAGO'        ),
    ( 'CO', 'BNPN_CO'                             , FALSE , 'CO-BNPN'        ),
    ( 'CO', 'REFINANCE_CO'                        , FALSE , 'CO-REFINANCE'   ),
    ( 'CO', 'EXPEDITED_CLIENT_PAGO_CO'            , FALSE , NULL             ),
    ( 'CO', 'CLIENT_CHECKOUT_PAGO_CO'             , FALSE , NULL             ),
    ( 'BR', 'PREAPPROVAL_PAGO_BR'                 , FALSE , 'BR-PREAPPROVAL' ),
    ( 'BR', 'PREAPPROVAL_CHECKPOINT_PAGO_BR'      , FALSE , 'BR-PREAPPROVAL' ),
    ( 'BR', 'PROSPECT_PAGO_BR'                    , FALSE , 'BR-PAGO'        ),
    ( 'BR', 'PROSPECT_PAGO_V2_BR'                 , FALSE , 'BR-PAGO'        ),
    ( 'BR', 'PROSPECT_CHECKPOINT_PAGO_BR'         , FALSE , 'BR-PAGO'        ),
    ( 'BR', 'CLIENT_PAGO_BR'                      , FALSE , 'BR-PAGO'        ),
    ( 'BR', 'PROSPECT_PAGO_BNPN_BR'               , FALSE , 'BR-BNPN'        ),
    ( 'BR', 'BNPN_PIX_BR'                         , FALSE , 'BR-BNPN'        ),
    ( 'BR', 'PROSPECT_PAGO_BNPN_BR_LEGACY'        , TRUE  , 'BR-BNPN'        )
    AS tab(country_code, synthetic_journey_name, is_synthetic, journey_homolog)