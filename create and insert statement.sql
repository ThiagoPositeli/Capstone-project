--The create tables statement only have to execute 1 time, same as insert. 
-- We DONT create and drop tables everyday in production.


-- dimensao dim_dimensoes

CREATE TABLE IF NOT EXISTS `sandbox-coe.trusted_ge_dataquality.dim_dimensoes` 
(id_dimensao int64,
 nm_dimensao string, 
 tipo_dimensao string, 
 desc_dimensao string, 
 dt_insert datetime)
partition by DATE (DT_INSERT)
OPTIONS(
  description= "Dimensão que contém as dimensões de data quality como 'completude', 'unicidade', 'razoabilidade', 'integridade', etc. Dados particionados por dt_insert ")
;


-- dimensao regras expectations

CREATE TABLE IF NOT EXISTS `sandbox-coe.trusted_ge_dataquality.dim_regras_expectations` 
(id_regra_expectation int64, 
 id_dimensao int64, nm_regra_expectation string, 
 desc_regra_expectation string, 
 tipo_regra_expectation  
 string, dt_insert datetime)
partition by DATE (DT_INSERT)
OPTIONS(
  description= "Dimensão que contém as regras de data quality criadas no Great Expectations. Dados particionados por dt_insert")
;


-- CRIANDO FATO DO MODELO.

-- fato ge data quality

CREATE TABLE IF NOT EXISTS `sandbox-coe.trusted_ge_dataquality.fato_ge_dataquality` 
(expectations_sucesso string,
 nm_suite string,
 resultado_sucesso string,
 nm_tabela string,
 nm_conexao_batch string,
 nm_source_execucao string,
 nm_execucao_expectation string,
 resultado_info_excecao string,
 resultado_info_traceback string,
 resultado_info_raised string,
 ft_nm_regra_expectation string,
 porcentagem_faltante string,
 porcentagem_inesperada string,
 porcentagem_nao_faltante string,
 porcentagem_inesperada_total string,
 id_batch_coluna string,
 nm_coluna string,
 porcentagem_limiar_aceite string,
 timestamp_execucao_expectation timestamp,
 qtd_elemento integer,
 qtd_elemento_faltante integer,
 qtd_elemento_inesperado integer,
 dt_insert_fato timestamp,
 id_regra_expectation integer,
 id_dimensao integer
 )
partition by DATE (dt_insert_fato)
OPTIONS(
  description= "Tabela fato que contém os dados das execuções das regras de data quality pelo Great Expectations, joinada pelas dimensoes onde se aplica as quebras do dado e contém dados particionados por dt_execucao, dt_validacao, dt_insert")
;


-- insert tabelas como única vez.



-- dim dimensoes


insert into `sandbox-coe.trusted_ge_dataquality.dim_dimensoes`
(id_dimensao, 
 nm_dimensao,  
 tipo_dimensao,
 desc_dimensao,
 dt_insert
 )

values
(1, 'completude', '', 'A completude é a proporção de dados existentes em relação a todos os atributos presentes. Em outras palavras, quanta informação está faltando em cada item dos nossos dados.', '2022-01-05 14:15:00'),
(2, 'conformidade', '', 'Essa dimensão de qualidade de dados representa o nível em que os dados estão de acordo com sua definição – formato, tipo, faixa de valores…', '2022-01-05 14:15:00'),
(3, 'unicidade','','A singularidade é uma dimensão de qualidade de dados que se refere a termos apenas um registro único de algo, baseado em um atributo ', '2022-01-05 14:15:00' ),
(4, 'consistencia','É medida pela diferença entre a comparação de duas ou mais representações de alguma coisa, em relação a uma definição.', '', '2022-01-05 14:15:00' ),
(5, 'periodicidade', '', 'É a capacidade que o dado tem de representar um momento no tempo', '2022-01-05 14:15:00'),
(6, 'acuracidade', '',  'Representa o quão corretamente os dados descrevem o “mundo real”', '2022-01-05 14:15:00')

;


-- dim regras expectations

insert into `sandbox-coe.trusted_ge_dataquality.dim_regras_expectations`
(id_regra_expectation,	
 id_dimensao,
 nm_regra_expectation,	
 desc_regra_expectation,
 tipo_regra_expectation,	
 dt_insert
 )
 values 
(1,3,	'expect_column_distinct_values_to_be_in_set', 'column aggregate expectation', '','2022-01-05 14:15:00'),
(2,	3,	'expect_column_distinct_values_to_contain_set',	'column aggregate expectation','', '2022-01-05 14:15:00'),
(3, 3, 	'expect_column_distinct_values_to_equal_set',	'column aggregate expectation','',	'2022-01-05 14:15:00'),
(4, 2, 	'expect_column_kl_divergence_to_be_less_than'	,	'column aggregate expectation','',	'2022-01-05 14:15:00'),
(5, 2, 	'expect_column_max_to_be_between',	'column aggregate expectation', '', '2022-01-05 14:15:00'),
(6,	2, 	'expect_column_mean_to_be_between',		'column aggregate expectation', '',	'2022-01-05 14:15:00'),
(7,	2,	'expect_column_median_to_be_between',		'column aggregate expectation', '', '2022-01-05 14:15:00'),
(8,	2, 	'expect_column_min_to_be_between',		'column aggregate expectation', '', '2022-01-05 14:15:00'),
(9, 2,	'expect_column_most_common_value_to_be_in_set',		'column aggregate expectation','',	'2022-01-05 14:15:00'),
(10,	2,	'expect_column_pair_values_a_to_be_greater_than_b', 'multi-column expectationneeds migration to modular expectations api', '',	'2022-01-05 14:15:00'),
(11, 	2, 	'expect_column_pair_values_to_be_equal',		'multi-column expectationneeds migration to modular expectations api', '', '2022-01-05 14:15:00'),
(12,	2,	'expect_column_pair_values_to_be_in_set',		'multi-column expectationneeds migration to modular expectations api', '', 	'2022-01-05 14:15:00'),
(13,	2,	'expect_column_proportion_of_unique_values_to_be_between',		'column aggregate expectation', '',	'2022-01-05 14:15:00'),
(14, 	2, 	'expect_column_quantile_values_to_be_between', 		'column aggregate expectation', '',	'2022-01-05 14:15:00'),
(15, 	2, 	'expect_column_stdev_to_be_between', 		'column aggregate expectation', '',	'2022-01-05 14:15:00'),
(16, 	2, 	'expect_column_sum_to_be_between',		'column aggregate expectation', '',	'2022-01-05 14:15:00'),
(17,	6,	'expect_column_to_exist',		'table expectation', '', 	'2022-01-05 14:15:00'),
(18,	2,	'expect_column_unique_value_count_to_be_between',		'column aggregate expectation', '', 	'2022-01-05 14:15:00'),
(19, 	2, 	'expect_column_value_lengths_to_be_between', 		'column map expectation', '',	'2022-01-05 14:15:00'),
(20, 	2, 	'expect_column_value_lengths_to_equal',		'column map expectation', '', 	'2022-01-05 14:15:00'),
(21,	2,	'expect_column_value_z_scores_to_be_less_than', 		'column map expectation', '', 	'2022-01-05 14:15:00'),
(22,	2,	'expect_column_values_to_be_between',		'column map expectation', '', '2022-01-05 14:15:00'),
(23, 	5, 	'expect_column_values_to_be_dateutil_parseable', 		'column map expectation', '', 	'2022-01-05 14:15:00'),
(24, 5,		'expect_column_values_to_be_decreasing',		'column map expectation', '',	'2022-01-05 14:15:00'),
(25, 	2, 	'expect_column_values_to_be_in_set', 		'column map expectation', '', 	'2022-01-05 14:15:00'),
(26, 	2, 	'expect_column_values_to_be_in_type_list',		'column map expectation', '',	'2022-01-05 14:15:00'),
(27, 	5, 	'expect_column_values_to_be_increasing', 		'column map expectation', '',	'2022-01-05 14:15:00'),
(28, 	1, 	'expect_column_values_to_be_json_parseable', 		'column map expectation', '',	'2022-01-05 14:15:00'),
(29,	1,	'expect_column_values_to_be_null',		'column map expectation', '',	'2022-01-05 14:15:00'),
(30,	6,	'expect_column_values_to_be_of_type',		'column map expectation',	'', '2022-01-05 14:15:00'),
(31, 	3, 	'expect_column_values_to_be_unique', 		'column map expectation', '',  	'2022-01-05 14:15:00'),
(32, 	4, 	'expect_column_values_to_match_json_schema', 		'column map expectation', '', 	'2022-01-05 14:15:00'),
(33, 	4,	'expect_column_values_to_match_like_pattern', 		'column map expectation', '', 	'2022-01-05 14:15:00'),
(34, 	2, 	'expect_column_values_to_match_like_pattern_list',		'column map expectation', '', 	'2022-01-05 14:15:00'),
(35, 	2, 	'expect_column_values_to_match_regex','','',			'2022-01-05 14:15:00'),
(36, 2, 	'expect_column_values_to_match_regex_list',		'column map expectation', '', 	'2022-01-05 14:15:00'),
(37, 	6, 	'expect_column_values_to_match_strftime_format',		'column map expectation', '', 	'2022-01-05 14:15:00'),
(38, 	1, 	'expect_column_values_to_not_be_in_set', 		'column map expectation', '',	'2022-01-05 14:15:00'),
(39, 	1, 	'expect_column_values_to_not_be_null', 		'column map expectation', '', 	'2022-01-05 14:15:00'),
(40, 	2, 	'expect_column_values_to_not_match_like_pattern',		'column map expectation', '',	'2022-01-05 14:15:00'),
(41, 	2, 	'expect_column_values_to_not_match_like_pattern_list', 'column map expectation', '',	'2022-01-05 14:15:00'),
(42, 	2,	 'expect_column_values_to_not_match_regex',		'column map expectation', '', 	'2022-01-05 14:15:00'),
(43,	2,	'expect_column_values_to_not_match_regex_list', 		'column map expectation', '', '2022-01-05 14:15:00'),
(44, 	3, 	'expect_compound_columns_to_be_unique', 		'multi-column expectationneeds migration to modular expectations api', '', 	'2022-01-05 14:15:00'),
(45, 	4,	'expect_multicolumn_sum_to_equal', 		'column aggregate expectationneeds migration to modular expectations api', '', 	'2022-01-05 14:15:00'),
(46, 	3, 	'expect_select_column_values_to_be_unique_within_record', 	'table expectationneeds migration to modular expectations api', '', '2022-01-05 14:15:00'),
(47, 	2, 	'expect_table_column_count_to_be_between', 		'table expectation', '',	'2022-01-05 14:15:00'),
(48,	2, 	'expect_table_column_count_to_equal',		'table expectation', '', 	'2022-01-05 14:15:00'),
(49, 	2, 	'expect_table_columns_to_match_ordered_list', 'table expectation','',			'2022-01-05 14:15:00'),
(50, 	2, 	'expect_table_columns_to_match_set', 	'table expectation', '', 	'2022-01-05 14:15:00'),
(51, 	2,	 'expect_table_row_count_to_be_between',		'table expectation', '', 	'2022-01-05 14:15:00'),
(52,	2,	'expect_table_row_count_to_equal', 		'table expectation', '', 	'2022-01-05 14:15:00'),
(53, 	4, 	'expect_table_row_count_to_equal_other_table', 		'table expectationmulti-table expectation', '',	'2022-01-05 14:15:00')
