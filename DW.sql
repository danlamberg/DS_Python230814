#Scripts de criação do DW (Modelo Multidimensional) INEP
create database dw_inep;
use dw_inep;

#Criação das tabelas Dimensão
drop table if exists dim_ano;
create table dim_ano
(
	tf_ano bigint,
    ano varchar(255)
);

drop table if exists dim_curso;
create table dim_curso
(
	tf_curso bigint,
    curso varchar(255)
);

drop table if exists dim_uf;
create table dim_uf
(
	tf_uf bigint,
    uf varchar(255)
);


drop table if exists dim_municipio;
create table dim_municipio
(
	tf_municipio bigint,
    municipio varchar(255)
);

drop table if exists dim_ies;
create table dim_ies
(
	tf_ies bigint,
    ies varchar(255)
);

drop table if exists dim_modalidade;
create table dim_modalidade
(
	tf_modalidade bigint,
    modalidade varchar(255)
);
#---------------------------------------
create table fact_matriculas
(
	matriculas int,
    tf_ano bigint,
    tf_curso bigint,
    tf_ies bigint,
    tf_uf bigint,
    tf_municipio bigint,
    tf_modalidade bigint
);

select * from dim_uf;
select * from dim_municipio;
truncate table dw_inep.dim_municipio;
truncate table fact_matriculas;

insert into fact_matriculas (matriculas, tf_municipio, tf_uf)
select * from
(select 100 as matriculas) as matriculas,
(select distinct tf_municipio from dim_municipio where municipio = 'Curitiba' limit 1) as tf_municipio,
(select distinct tf_uf from dim_uf where uf = 'Paraná' limit 1) as tf_uf;

select * from fact_matriculas;

truncate table dim_modalidade;
select * from dim_modalidade;

select * from dim_curso;
select * from dim_ano;

truncate table dim_ies;
select * from dim_ies;

SELECT COUNT(*) FROM dim_ies;
SELECT COUNT(*) FROM dim_ano;


