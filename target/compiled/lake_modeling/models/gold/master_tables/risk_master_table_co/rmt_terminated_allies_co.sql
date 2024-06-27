

  with apps as (
    select
      a.ally_name as ally_slug,
      coalesce(a.origination_date, a.application_date) :: date as d_vintage,
      loan_id,
      case
        when (
          a.approved_amount is null
          or a.approved_amount = 0
        ) then a.requested_amount
        else a.approved_amount
      end as amount,
      case
        when a.journey_name like '%SANTANDER%'
        and (
          a.journey_stages like '%underwriting-co%'
          or a.journey_stages like '%underwriting-psychometric-co%'
        ) then 'FINANCIA_CO'
        when a.journey_name like '%SANTANDER%' then 'SANTANDER_CO'
        when a.product is not null
        and a.product like '%PAGO%' then 'PAGO_CO'
        when a.product is not null
        and a.product like '%FINANCIA%' then 'FINANCIA_CO'
        when a.journey_name is not null
        and a.journey_name like '%PAGO%' then 'PAGO_CO'
        when a.journey_name is not null
        and a.journey_name like '%FINANCIA%' then 'FINANCIA_CO'
        when a.credit_policy_name in (
          'addipago_0aprfga_policy',
          'addipago_0fga_policy',
          'addipago_claro_policy',
          'addipago_mario_h_policy',
          'addipago_no_history_policy',
          'addipago_policy',
          'addipago_policy_amoblando',
          'adelante_policy_pago',
          'closing_policy_pago',
          'default_policy_pago',
          'finalization_policy_pago',
          'rc_0aprfga',
          'rc_0fga',
          'rc_addipago_policy_amoblando',
          'rc_adelante_policy',
          'rc_closing_policy',
          'rc_finalization_policy',
          'rc_pago_0aprfga',
          'rc_pago_0fga',
          'rc_pago_claro',
          'rc_pago_mario_h',
          'rc_pago_standard',
          'rc_rejection_policy',
          'rc_standard',
          'rejection_policy_pago'
        ) then 'PAGO_CO'
        when a.term > 3 then 'FINANCIA_CO'
        when a.credit_policy_name in (
          'closing_policy',
          'finalization_policy',
          'rejection_policy'
        )
        and a.term = 3 then 'PAGO_CO'
        when a.credit_policy_name is null
        and a.term > 3 then 'FINANCIA_CO'
        when a.credit_policy_name is null
        and a.approved_amount <= 600000 then 'PAGO_CO'
        when a.credit_policy_name is not null then 'FINANCIA_CO'
        else 'FINANCIA_CO'
      end as product,
      d.brand :['name'] ['value'] as ally_brand
    from
      cur.applications a
      left join cur.allies d on a.ally_name = d.slug
  ),
  last_origination_date as (
    select
      ally_slug,
      first_value(d_vintage) over (
        partition by ally_slug
        order by
          d_vintage desc
      ) as last_day
    from
      apps
    where
      loan_id is not null
    group by
      1,
      d_vintage
  ),
  first_day as (
    select
      ally_slug,
      min(d_vintage) as first_day
    from
      apps
    group by
      1
  ),
  last_date as (
    select
      distinct a.ally_slug,
      b.last_day
    from
      apps as a
      left join last_origination_date as b on a.ally_slug = b.ally_slug
    where
      a.ally_brand in (
    'Tienda AO',
    'Moviles y Partes SAS',
    'LTE de Colombia',
    'Marflex',
    'Colombia Internacional',
    'TELEPLUS',
    'Colbiker',
    'Clinicel',
    'Bikers Paradise',
    'Tecno Sports',
    'Sport Bikes',
    'Mega Guay',
    'Manhattan',
    'Tienda Iphone BQ',
    'Ktegno',
    'Celumacch',
    'Kreditkasa',
    'MercaTecno',
    'Celumania DyE',
    'DISCOMTECH',
    'Mastronics',
    'Play Station Campanario',
    'Cell Phone Apple',
    'Tiendas Saga Bike SA',
    'Mr Apple',
    'Daca Tecnologia',
    'Movilworld',
    'Celu Boyaca',
    'Mi Movil Ya',
    'Credisoluciones',
    'TU MEJOR OFERTA',
    'OPENMOVIL.COM',
    'Tu Punto Electro',
    'Celltronic',
    'Tecnomania',
    'Brujula Digital',
    'Tu smartphone',
    'Iphoneshop Bogota',
    'Impresistem',
    'Ciclo Japon',
    'Naturals Home SAS',
    'Celulares Legales',
    'CeluMovil Store',
    'Credi Madrid 2',
    'Migo',
    'Evolution Idiomas',
    'JFG Comunicaciones',
    'Green Energy Motors',
    'Luz Mary planes moviles hogar y pymes',
    'Crediexpress',
    'DISTRI TODO CALI',
    'Checheres',
    'Vainilla Lenceria',
    'Esmeraldas Colombia',
    'Wizz Life SAS',
    'Korolos Comercializadora Hvg SAS',
    'THE POWER MBA',
    'Grupo Colprende S.A.S',
    'Project Nomad',
    'Bici Sport',
    'Dislacasita',
    'WellDone',
    'Xtreme Hardware',
    'Casa de Apple',
    'MOBILE STORE 84 SAS',
    'Colchones Carino',
    'Bikers Paradise',
    'Tecno Sports',
    'Sport Bikes',
    'Mega Guay',
    'Manhattan',
    'La pipa comercializadora SAS',
    'PLADANIBAG'
      )
      or a.ally_slug in (
   "betigo-online",
    "aquitaco-ecommerce",
    "aquitaco-online",
    'mundomovil-online',
    'mundomovilsas-online',
    'cls-ecommerce',
    'cls-online',
    'kreditkasa-ecommerce',
    'kreditkasa-online',
    'celucomunicacioneselpaisita-online',
    'deko-online',
    'deko-ecommerce',
    'argujoyas-online',
    'qweenbyreynazayas-online',
    'ohanacosmeticshop-online',
    'conectividadmovilsas-online',
    'sygtech-ecommerce',
    'sygtech-online',
    'antojoscom-online',
    'elpalaciodelatecnologia-online',
    'ciudadmovilcolombia-online',
    'llavescell-online',
    'cellplanet-online',
    'colchonesboxi-ecommerce',
    'colchonesremyboxi-online',
    'colchonesrem-ecommerce',
    'joyeriadeleje-online',
    'joyeriadeleje-ecommerce',
    'calzadojeepettos-online',
    'digitalmac-ecommerce',
    'digitalmac-online',
    'sybcolombia-ecommerce',
    'sybcolombia-online',
    'accemovil-online',
    'alored-online',
    'btechnology-ecommerce',
    'btechnology-online',
    'cambiosystems-online',
    'cellsertelecomunicaciones-online',
    'cellsertelecomunicaciones-ecommerce',
    'celufacil-online',
    'celularesfenix-online',
    'celularesfenix-ecommerce',
    'celularexpress-online',
    'centerphone-online',
    'clevercel-online',
    'cxcenter-online',
    'cxcenter-ecommerce',
    'cyfmayorista-online',
    'detodoparaelhogar-online',
    'digitthau-ecommerce',
    'digitthaulink-online',
    'distritec-online',
    'enavii-ecommerce',
    'felestdistribuciones-online',
    'giftronic-ecommerce',
    'giftronic-online',
    'grupoempresarialgrowsas-online',
    'importacionescristian-online',
    'ingelectronixma-online',
    'ingelectronixma-ecommerce',
    'iphonecell-online',
    'julicell-online',
    'kebbin-ecommerce',
    'mialmacen-online',
    'movilestienda-online',
    'moviscell-online',
    'movisim-online',
    'mundosmartphone-ecommerce',
    'mundosmartphone-online',
    'mymtecnologiadepunta-online',
    'mymtecnologiadepunta-ecommerce - ally does not exist',
    'mymtecnologiadepuntaweb-ecommerce',
    'ozonoequipos-online',
    'pochocell-online',
    'quintoelemento-online',
    'rincontecno-online',
    'rincontecno-ecommerce',
    'technologyspace-online',
    'tecnocell-online',
    'teknopolispolux-ecommerce',
    'tiendatecno-online',
    'tumovil-online',
    'voipcellmaicao-online',
    'workcom-online',
    'workcom-ecommerce',
    'worldofgame-online',
    'zonadigitalsur-online',
    'centerphone-ecommerce',
    'dyaimportaciones-online',
    'dyaimportaciones-ecommerce',
    'dygimportaciones-ecommerce',
    'comercializadoracelislunac-online',
    'ryangrupoempresarial-ecommerce',
    'ryangrupoempresarial-online',
    'tegnosim-online',
    'tiendamsicolombia-online',
    'shop22-online',
    'calzadojeepettos-online'
      )
  )
  select
    b.ally_slug,
    b.last_day,
    a.daily_GMV_PAGO,
    c.daily_GMV_FINANCIA,
    d.first_day
  from
    last_date as b
    left join (
      select
        ally_slug,
        d_vintage,
        round(sum(amount) / 1000000, 1) as daily_GMV_PAGO
      from
        apps
      where
        loan_id is not null
        and product = "PAGO_CO"
      group by
        1,
        2
    ) as a on a.ally_slug = b.ally_slug
    and a.d_vintage = b.last_day
    left join (
      select
        ally_slug,
        d_vintage,
        round(sum(amount) / 1000000, 1) as daily_GMV_FINANCIA
      from
        apps
      where
        loan_id is not null
        and product = "FINANCIA_CO"
      group by
        1,
        2
    ) as c on c.ally_slug = b.ally_slug
    and c.d_vintage = b.last_day
    left join first_day as d on a.ally_slug = d.ally_slug