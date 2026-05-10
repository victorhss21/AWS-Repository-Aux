with

-----------------------------------------------------------------------------------------------------
-- PJ PREMIUM | QUANTIDADE DE CLIENTES POR VARIAÇÃO DE VOL PAG 3M POR ANOMES
-----------------------------------------------------------------------------------------------------

-- Calculando lag 1 do volume_3m por cliente
tbl_cnpj_premium_vol_pag_3m_lag as (
select
    *,
    lag(valor_pag_3m,1) over(partition by cnpj_cpf_orig order by anomes asc) as valor_pag_3m_lag_1
from database_workspace_db.tbl_cnpj_premium_vol_pag_3m_v2
where 1=1
    and anomes between 202501 and 202512
)

-- Calculando variação MoM por Vol 3M por cliente
,tbl_cnpj_premium_vol_pag_3m_mom as (
select
    anomes,
    cnpj_cpf_orig,
    valor_pag_3m,
    valor_pag_3m_lag_1,
    case
        when (valor_pag_3m/valor_pag_3m_lag_1)-1 < 0.15 then 'Baixa_Variacao_MoM'
        when (valor_pag_3m/valor_pag_3m_lag_1)-1 >= 0.15 and (valor_pag_3m/valor_pag_3m_lag_1)-1 <= 0.50 then 'Relevante_Variacao_MoM' 
        when (valor_pag_3m/valor_pag_3m_lag_1)-1 < 0.15 then 'Alta_Variacao_MoM'
        else '-'
    end as intensidade_varicao_vol_3m_mom
from tbl_cnpj_premium_vol_pag_3m_lag
)

-- Contando qtd de clientes por categoria de variacao por anomes
,tbl_csld_qtd_clie_pj_intesidade_variacao as (
select
    anomes,
    intensidade_varicao_vol_3m_mom,
    'PJ' as tipo_pessoa,
    count(distinct cnpj_cpf_orig) as qtd_clie_dstc,
    count(cnpj_cpf_orig) as qtd_clie
from tbl_cnpj_premium_vol_pag_3m_mom
group by 1,2
)

-----------------------------------------------------------------------------------------------------
-- PF | QUANTIDADE DE CLIENTES POR VARIAÇÃO DE VOL PAG 3M POR ANOMES
-----------------------------------------------------------------------------------------------------

-- Calculando lag 1 do volume_3m por cliente
,tbl_cpf_vol_pag_3m_lag as (
select
    *,
    lag(valor_pag_3m,1) over(partition by cnpj_cpf_orig order by anomes asc) as valor_pag_3m_lag_1
from database_workspace_db.tbl_cpf_vol_pag_3m_v2
where 1=1
    and anomes between 202501 and 202512
)

-- Calculando variação MoM por Vol 3M por cliente
,tbl_cpf_vol_pag_3m_mom as (
select
    anomes,
    cnpj_cpf_orig,
    valor_pag_3m,
    valor_pag_3m_lag_1,
    case
        when (valor_pag_3m/valor_pag_3m_lag_1)-1 < 0.15 then 'Baixa_Variacao_MoM'
        when (valor_pag_3m/valor_pag_3m_lag_1)-1 >= 0.15 and (valor_pag_3m/valor_pag_3m_lag_1)-1 <= 0.50 then 'Relevante_Variacao_MoM' 
        when (valor_pag_3m/valor_pag_3m_lag_1)-1 < 0.15 then 'Alta_Variacao_MoM'
        else '-'
    end as intensidade_varicao_vol_3m_mom
from tbl_cpf_vol_pag_3m_lag
)

-- Contando qtd de clientes por categoria de variacao por anomes
,tbl_csld_qtd_clie_pf_intesidade_variacao as (
select
    anomes,
    intensidade_varicao_vol_3m_mom,
    'PF' as tipo_pessoa,
    count(distinct cnpj_cpf_orig) as qtd_clie_dstc,
    count(cnpj_cpf_orig) as qtd_clie
from tbl_cpf_vol_pag_3m_mom
group by 1,2
)

------------------------------------------------------------------------------------------------------c
-- RESULTADO FINAL | EMPILHANDO TABELAS
-----------------------------------------------------------------------------------------------------

select *
from tbl_csld_qtd_clie_pj_intesidade_variacao
union all
select *
from tbl_csld_qtd_clie_pf_intesidade_variacao