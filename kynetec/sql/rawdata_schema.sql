-- rawdata definition

CREATE SCHEMA IF NOT EXISTS rawdata;

-- rawdata.rawdata_base_mercado definition

-- DROP TABLE rawdata.rawdata_base_mercado;
CREATE TABLE IF NOT EXISTS rawdata.rawdata_base_mercado (
	"CATEGORIA" text NULL,
	"CULTURA" text NULL,
	"SAFRA" text NULL,
	"SAFRA_UNIFICADA" text NULL,
	"COD_IBGE" text NULL,
	"REGIÃO" text NULL,
	"SEGMENTO_AGRUPADO_DISTRIBUIÇÃO" text NULL,
	"SUBSEGMENTO_DISTRIBUIÇÃO" text NULL,
	"SEGMENTO_BIOLÓGICOS" text NULL,
	"TIPO_PRODUCAO" text NULL,
	"CURVA_ABC" text NULL,
	"GRUPO_QUÍMICO" text NULL,
	"ALVO_GRUPO" text NULL,
	"ALVO_PRINCIPAL" text NULL,
	"ALVO_CIENTÍFICO" text NULL,
	"LOCAL_COMPRA_GRUPO" text NULL,
	"EPOCA_DECISÃO" text NULL,
	"EPOCA_COMPRA" text NULL,
	"FAIXA_IDADE" text NULL,
	"TEMPO_EXPERIENCIA" text NULL,
	"GENERO" text NULL,
	"FAIXA_AREA_CULTURA" text NULL,
	"FORMACAO" text NULL,
	"FAIXA_AREA_GRUPO" text NULL,
	"SEQUENCIA" text NULL,
	"AREA" text NULL,
	"VOLUME" text NULL,
	"VALOR_DE_MERCADO_MI_RS" text NULL,
	"VALOR_DE_MERCADO_MI_USD" text NULL
);

-- rawdata.rawdata_base_local_compra definition

-- DROP TABLE rawdata.rawdata_base_local_compra;
CREATE TABLE IF NOT EXISTS rawdata.rawdata_base_local_compra (
	"CATEGORIA" text NULL,
	"CULTURA" text NULL,
	"SAFRA" text NULL,
	"SAFRA_UNIFICADA" text NULL,
	"COD_IBGE" text NULL,
	"SEGMENTO_AGRUPADO_DISTRIBUIÇÃO" text NULL,
	"SUBSEGMENTO_DISTRIBUIÇÃO" text NULL,
	"LOCAL_COMPRA_GRUPO" text NULL,
	"LOCAL_COMPRA" text NULL,
	"CURVA_ABC" text NULL,
	"TIPO_PRODUCAO" text NULL,
	"FAIXA_AREA_GRUPO" text NULL,
	"AREA" text NULL,
	"VALOR_DE_MERCADO_MI_RS" text NULL,
	"IDLOCAL_COMPRA" text NULL
);

-- rawdata.rawdata_base_indicadores_bip_reduzida definition

-- DROP TABLE rawdata.rawdata_base_indicadores_bip_reduzida;
CREATE TABLE rawdata.rawdata_base_indicadores_bip_reduzida (
	"CULTURA" text NULL,
	"COD_IBGE" text NULL,
	"REGIÃO" text NULL,
	"SAFRA" text NULL,
	"SAFRA_UNIFICADA" text NULL,
	"CATEGORIA" text NULL,
	"SEGMENTO_AGRUPADO_DISTRIBUIÇÃO" text NULL,
	"SUBSEGMENTO_DISTRIBUIÇÃO" text NULL,
	"CURVA_ABC" text NULL,
	"TIPO_PRODUCAO" text NULL,
	"LOCAL_COMPRA_GRUPO" text NULL,
	"FAIXA_AREA_GRUPO" text NULL,
	"ADOÇÃO_N" text NULL,
	"ADOÇÃO" text NULL,
	"APLICAÇÕES" text NULL,
	"PRODUTOS_MISTURA" text NULL,
	"CUSTO_RS" text NULL,
	"ÁREA_CULTIVADA_1000_HA" text NULL,
	"ÁREA_POTENCIAL_TRATADA_1000_HA" text NULL,
	"VALOR_DE_MERCADO_MI_RS" text NULL,
	"DOSE_HA" text NULL
);

-- rawdata.transform_base_mercado(text) definition

--DROP PROCEDURE rawdata.transform_base_mercado(text);
CREATE OR REPLACE PROCEDURE rawdata.transform_base_mercado("mode" text DEFAULT 'append')
LANGUAGE plpgsql AS
$procedure$
	BEGIN 
		IF "mode" = 'replace' THEN
			RAISE NOTICE 'Truncating table distribution.f_base_mercado_encoded ...';
			TRUNCATE distribution.f_base_mercado_encoded RESTART IDENTITY;
		END IF;
		
		RAISE NOTICE 'Inserting data into distribution.f_base_mercado_encoded ...';
		INSERT INTO distribution.f_base_mercado_encoded
		(categoria, cultura, safra, safra_unificada, cod_ibge, regiao, segmento_agrupado, subsegmento, 
		 segmento_biologicos, tipo_producao, curva_abc, grupo_quimico, alvo_grupo, alvo_principal, alvo_cientifico, 
		 local_compra_grupo, epoca_decisao, epoca_compra, faixa_idade, tempo_experiencia, genero, faixa_area_cultura, 
		 formacao, faixa_area_grupo, sequencia, area, volume, valor_de_mercado_mi_rs, valor_de_mercado_mi_usd)
		 SELECT categ.id, cult.id, safra.id, safra_u.id, 
		 		"COD_IBGE"::INT, 
		 		reg.id, seg_ag.id, sub_seg.id, 
		 	    seg_b.id, tp_prod.id, abc.id, g_quim.id, alv_g.id, alv_p.id, alv_c.id, loc_g.id, epoc_d.id, 
		 	    epoc_c.id, idade.id, t_exp.id, gen.id, fac.id, form.id, fag.id, 
		 	    COALESCE(seq.id, 93), 
		 	    replace("AREA",',','.')::numeric,
		 	    replace("VOLUME",',','.')::numeric, 
		 	    replace("VALOR_DE_MERCADO_MI_RS",',','.')::numeric, 
		 	    replace("VALOR_DE_MERCADO_MI_USD",',','.')::numeric
		FROM rawdata.rawdata_base_mercado bm
		JOIN distribution.d_categoria categ ON bm."CATEGORIA" = categ.categoria_orig
		JOIN distribution.d_cultura cult ON bm."CULTURA" = cult.cultura
		JOIN distribution.d_safra safra ON bm."SAFRA"::int = safra.safra
		JOIN distribution.d_safra_unificada safra_u ON bm."SAFRA_UNIFICADA"::int = safra_u.safra_unificada
		JOIN distribution.d_regiao reg ON bm."REGIÃO" = reg.regiao
		JOIN distribution.d_segmento_agrupado seg_ag ON bm."SEGMENTO_AGRUPADO_DISTRIBUIÇÃO" = seg_ag.segmento_agrupado_orig
		JOIN distribution.d_subsegmento sub_seg ON bm."SUBSEGMENTO_DISTRIBUIÇÃO" = sub_seg.subsegmento_orig
		JOIN distribution.d_segmento_biologicos seg_b ON bm."SEGMENTO_BIOLÓGICOS" = seg_b.segmento_biologicos
		JOIN distribution.d_tipo_producao tp_prod ON bm."TIPO_PRODUCAO" = tp_prod.tipo_producao_orig
		JOIN distribution.d_curva_abc abc ON bm."CURVA_ABC" = abc.curva_abc_orig
		JOIN distribution.d_grupo_quimico g_quim ON bm."GRUPO_QUÍMICO" = g_quim.grupo_quimico
		JOIN distribution.d_alvo_grupo alv_g ON bm."ALVO_GRUPO" = alv_g.alvo_grupo
		JOIN distribution.d_alvo_principal alv_p ON bm."ALVO_PRINCIPAL" = alv_p.alvo_principal_orig
		JOIN distribution.d_alvo_cientifico alv_c ON bm."ALVO_CIENTÍFICO" = alv_c.alvo_cientifico
		JOIN distribution.d_local_compra_grupo loc_g ON bm."LOCAL_COMPRA_GRUPO" = loc_g.local_compra_grupo
		JOIN distribution.d_epoca_decisao epoc_d ON bm."EPOCA_DECISÃO" = epoc_d.epoca_decisao_orig
		JOIN distribution.d_epoca_compra epoc_c ON bm."EPOCA_COMPRA" = epoc_c.epoca_compra_orig
		JOIN distribution.d_faixa_idade idade ON bm."FAIXA_IDADE" = idade.faixa_idade_orig
		JOIN distribution.d_tempo_experiencia t_exp ON bm."TEMPO_EXPERIENCIA" = t_exp.tempo_experiencia
		JOIN distribution.d_genero gen ON bm."GENERO" = gen.genero
		JOIN distribution.d_faixa_area_cultura fac ON bm."FAIXA_AREA_CULTURA" = fac.faixa_area_cultura_orig
		JOIN distribution.d_formacao form ON bm."FORMACAO" = form.formacao
		JOIN distribution.d_faixa_area_grupo fag ON bm."FAIXA_AREA_GRUPO" = fag.faixa_area_grupo
		LEFT JOIN distribution.d_sequencia seq ON bm."SEQUENCIA" = seq.sequencia;
		RAISE NOTICE 'Process complete.';
	END;
$procedure$;

-- rawdata.transform_base_local_compra(text) definition

--DROP PROCEDURE rawdata.transform_base_local_compra(text);
CREATE OR REPLACE PROCEDURE rawdata.transform_base_local_compra("mode" text DEFAULT 'append')
LANGUAGE plpgsql AS
$procedure$
	BEGIN 
		IF "mode" = 'replace' THEN
			RAISE NOTICE 'Truncating table distribution.f_base_local_compra_encoded ...';
			TRUNCATE distribution.f_base_local_compra_encoded;
		END IF;
		
		RAISE NOTICE 'Inserting data into distribution.f_base_local_compra_encoded ...';
		INSERT INTO distribution.f_base_local_compra_encoded
		(categoria, cultura, safra, safra_unificada, cod_ibge, segmento_agrupado, 
		subsegmento, local_compra_grupo, local_compra, curva_abc, tipo_producao, 
		faixa_area_grupo, area, valor_de_mercado_mi_rs)
		SELECT categ.id, cult.id, safra.id, safra_u.id, 
			   "COD_IBGE"::int, 
			   seg_ag.id, sub_seg.id, lcg.id, 
			   "IDLOCAL_COMPRA"::int, 
			   abc.id, tp_prod.id, fag.id,  
			   replace("AREA",',','.')::numeric, 
			   replace("VALOR_DE_MERCADO_MI_RS",',','.')::numeric
		FROM rawdata.rawdata_base_local_compra blc
		JOIN distribution.d_categoria categ ON blc."CATEGORIA" = categ.categoria_orig
		JOIN distribution.d_cultura cult ON blc."CULTURA" = cult.cultura
		JOIN distribution.d_safra safra ON blc."SAFRA"::int = safra.safra
		JOIN distribution.d_safra_unificada safra_u ON blc."SAFRA_UNIFICADA"::int = safra_u.safra_unificada
		JOIN distribution.d_segmento_agrupado seg_ag ON blc."SEGMENTO_AGRUPADO_DISTRIBUIÇÃO" = seg_ag.segmento_agrupado_orig
		JOIN distribution.d_subsegmento sub_seg ON blc."SUBSEGMENTO_DISTRIBUIÇÃO" = sub_seg.subsegmento_orig
		JOIN distribution.d_curva_abc abc ON blc."CURVA_ABC" = abc.curva_abc_orig
		JOIN distribution.d_tipo_producao tp_prod ON blc."TIPO_PRODUCAO" = tp_prod.tipo_producao_orig
		JOIN distribution.d_faixa_area_grupo fag ON blc."FAIXA_AREA_GRUPO" = fag.faixa_area_grupo
		JOIN distribution.d_local_compra_grupo lcg ON blc."LOCAL_COMPRA_GRUPO" = lcg.local_compra_grupo;
		RAISE NOTICE 'Process complete.';
	END;
$procedure$;

-- rawdata.transform_base_indicadores_bip_reduzida(text) definition

--DROP PROCEDURE rawdata.transform_base_indicadores_bip_reduzida(text);
CREATE OR REPLACE PROCEDURE rawdata.transform_base_indicadores_bip_reduzida("mode" text DEFAULT 'append')
LANGUAGE plpgsql AS
$procedure$
	BEGIN 
		IF "mode" = 'replace' THEN
			RAISE NOTICE 'Truncating table distribution.f_base_local_compra_encoded ...';
			TRUNCATE distribution.f_base_indicadores_bip_reduzida_teste;
		END IF;
		
		RAISE NOTICE 'Inserting data into distribution.f_base_local_compra_encoded ...';
		INSERT INTO distribution.f_base_indicadores_bip_reduzida_encoded_teste
		(cultura, cod_ibge, regiao, safra, safra_unificada, categoria, segmento_agrupado, subsegmento,
		curva_abc, tipo_producao, local_compra_grupo, faixa_area_grupo, adocao_n, adocao, aplicacoes,
		produtos_mistura, custo_rs, area_cultivada_1000_ha, area_potencial_tratada_1000_ha, 
		valor_de_mercado_mi_rs, dose_ha, area)
		SELECT cult.id, "COD_IBGE"::int, reg.id, safra.id, safra_u.id, categ.id, 
			   seg_ag.id, sub_seg.id, abc.id, tp_prod.id, lcg.id, fag.id,
			   replace("ADOÇÃO_N",',','.')::numeric,
			   replace("ADOÇÃO",',','.')::numeric,
			   CASE "CATEGORIA"
				   WHEN 'SEMENTES' THEN 1
				   ELSE  replace(nullif("APLICAÇÕES",' '),',','.')::numeric	   	
			   END,
			   CASE "CATEGORIA"
			   	   WHEN 'SEMENTES' THEN 1
			   	   ELSE replace(nullif("PRODUTOS_MISTURA",' '),',','.')::numeric
			   END,	   
			   replace(nullif("CUSTO_RS",' '),',','.')::numeric,
			   replace("ÁREA_CULTIVADA_1000_HA",',','.')::numeric,
			   replace(nullif("ÁREA_POTENCIAL_TRATADA_1000_HA",' '),',','.')::numeric,
			   replace("VALOR_DE_MERCADO_MI_RS",',','.')::numeric,
			   replace(nullif("DOSE_HA",' '),',','.')::numeric,
			   CASE "CATEGORIA"
			   		WHEN 'SEMENTES' THEN replace("ÁREA_CULTIVADA_1000_HA",',','.')::numeric
			   		ELSE replace(nullif("ÁREA_POTENCIAL_TRATADA_1000_HA",' '),',','.')::numeric
			   END
		FROM rawdata.rawdata_base_indicadores_bip_reduzida bipr
		JOIN distribution.d_cultura cult ON bipr."CULTURA" = cult.cultura
		JOIN distribution.d_regiao reg ON bipr."REGIÃO" = reg.regiao
		JOIN distribution.d_safra safra ON bipr."SAFRA"::int = safra.safra
		JOIN distribution.d_safra_unificada safra_u ON bipr."SAFRA_UNIFICADA"::int = safra_u.safra_unificada
		JOIN distribution.d_categoria categ ON bipr."CATEGORIA" = categ.categoria_orig
		JOIN distribution.d_segmento_agrupado seg_ag ON bipr."SEGMENTO_AGRUPADO_DISTRIBUIÇÃO" = seg_ag.segmento_agrupado_orig
		JOIN distribution.d_subsegmento sub_seg ON bipr."SUBSEGMENTO_DISTRIBUIÇÃO" = sub_seg.subsegmento_orig
		JOIN distribution.d_curva_abc abc ON bipr."CURVA_ABC" = abc.curva_abc_orig
		JOIN distribution.d_tipo_producao tp_prod ON bipr."TIPO_PRODUCAO" = tp_prod.tipo_producao_orig
		JOIN distribution.d_faixa_area_grupo fag ON bipr."FAIXA_AREA_GRUPO" = fag.faixa_area_grupo
		JOIN distribution.d_local_compra_grupo lcg ON bipr."LOCAL_COMPRA_GRUPO" = lcg.local_compra_grupo;
		RAISE NOTICE 'Process complete.';
	END;
$procedure$;

