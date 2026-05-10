-- [Athena SQL] Criando a tabela refinada a nível PF e seus respectivos volumes
select
    anomes,
    cnpj_cpf_orig,
    tipo_pessoa_orig,
    cluster_seg_cnpj_cpf_orig,
    sum(cast(valor as decimal(20,2))) as valor_pag_3m
from "database-layer-raw"."dados_sinteticos_fn" 
where 1=1
    and tipo_pessoa_orig = 'J'
    and cluster_seg_cnpj_cpf_orig = 'PREMIUM'
    and anomes between {anomes_menos_2} and {anomes}
group by 1,2,3,4
order by 1 asc
