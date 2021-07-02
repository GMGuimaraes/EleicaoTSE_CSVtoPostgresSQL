const fs = require('fs');
const csv = require('csv-parser'); 
const pool = require('../db/connection');
let part = []

async function run(){
 
  const nomeArquivos =  fs.readdirSync('./TabelasCSV/consulta_cand_2020');
  const arquivos = nomeArquivos.filter( (element) =>{
    return   element !== 'leiame.pdf';
  })

      fs.createReadStream('./TabelasCSV/consulta_cand_2020/consulta_cand_2020_AL.csv')
      .pipe(csv({
          separator: ';',
          mapHeaders: ({ header, index }) => header.toLowerCase(),
          
        }))
        .on('data',  (data)   =>  {
          const {sq_candidato} = data;
          
            if(part.includes(sq_candidato) || sq_candidato == null){
              
            }else{
              part.push(sq_candidato)
               inserir(data)

            }
        })
  
}

 async function inserir(obj){

  pool.connect();
  const quer = `
  INSERT INTO eleicao.consulta_cand(
    ano_elecao, cd_tipo_eleicao, nm_tipo_eleicao, cd_eleicao, ds_eleicao, 
    dt_eleicao, tp_abrangencia, sg_uf, sg_ue, nm_ue, 
    cd_cargo, ds_cargo, sq_candidato, nr_candidato, 
    nm_candidato, nm_urna_candidato, nm_social, nr_cpf_candidato, 
    nm_email, cd_situacao_candidatura, ds_situacao_candidatura, 
    dc_detalhe_situacao_cand, ds_detalhe_situacao_cand, 
    tp_agremiacao, nr_partido, sq_coligacao, cd_nacionalidade, 
    ds_nacionalidade, sg_uf_nascimento, cd_municipio, 
    nm_municipio_nascimento, dt_nascimento, nr_idade_data_posse, 
    nr_titulo_eleitoral_candidato, cd_genero, ds_genero, cd_grau_instrucao, 
    ds_grau_instrucao, cd_estado_civil, cd_cor_raca, ds_cor_raca, cd_ocupacao, 
    ds_ocupacao, vr_despesa_max_campanha, cd_sit_tot_turno, ds_sit_tot_turno, 
    st_reeleicao, st_declarar_bens, nr_protocolo_candidatura, nr_processo, 
    cd_situacao_candidato_pleito, ds_situacao_candidato_pleito, cd_situacao_candidato_urna, 
    ds_situacao_candidato_urna, st_candidato_inserido_urna)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, 
      $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36
      ,$37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, 
      $51, $52, $53, $54, $55);
  `
  const {
    ano_eleicao, cd_tipo_eleicao, nm_tipo_eleicao, cd_eleicao, ds_eleicao, 
    dt_eleicao, tp_abrangencia, sg_uf, sg_ue, nm_ue, 
    cd_cargo, ds_cargo, sq_candidato, nr_candidato, 
    nm_candidato, nm_urna_candidato, nm_social_candidato, nr_cpf_candidato, 
    nm_email, cd_situacao_candidatura, ds_situacao_candidatura, 
    cd_detalhe_situacao_cand, ds_detalhe_situacao_cand, 
    tp_agremiacao, nr_partido, sq_coligacao, cd_nacionalidade, 
    ds_nacionalidade, sg_uf_nascimento, cd_municipio_nascimento, 
    nm_municipio_nascimento, dt_nascimento, nr_idade_data_posse, 
    nr_titulo_eleitoral_candidato, cd_genero, ds_genero, cd_grau_instrucao, 
    ds_grau_instrucao, cd_estado_civil, cd_cor_raca, ds_cor_raca, cd_ocupacao, 
    ds_ocupacao, vr_despesa_max_campanha, cd_sit_tot_turno, ds_sit_tot_turno, 
    st_reeleicao, st_declarar_bens, nr_protocolo_candidatura, nr_processo, 
    cd_situacao_candidato_pleito, ds_situacao_candidato_pleito, cd_situacao_candidato_urna, 
    ds_situacao_candidato_urna, st_candidato_inserido_urna
  } = obj;
 
  const value = [ano_eleicao, cd_tipo_eleicao, nm_tipo_eleicao, cd_eleicao, ds_eleicao, 
    dt_eleicao, tp_abrangencia, sg_uf, sg_ue, nm_ue, 
    cd_cargo, ds_cargo, sq_candidato, nr_candidato, 
    nm_candidato, nm_urna_candidato, nm_social_candidato, nr_cpf_candidato, 
    nm_email, cd_situacao_candidatura, ds_situacao_candidatura, 
    cd_detalhe_situacao_cand, ds_detalhe_situacao_cand, 
    tp_agremiacao, nr_partido, sq_coligacao, cd_nacionalidade, 
    ds_nacionalidade, sg_uf_nascimento, cd_municipio_nascimento, 
    nm_municipio_nascimento, dt_nascimento, nr_idade_data_posse, 
    nr_titulo_eleitoral_candidato, cd_genero, ds_genero, cd_grau_instrucao, 
    ds_grau_instrucao, cd_estado_civil, cd_cor_raca, ds_cor_raca, cd_ocupacao, 
    ds_ocupacao, vr_despesa_max_campanha, cd_sit_tot_turno, ds_sit_tot_turno, 
    st_reeleicao, st_declarar_bens, nr_protocolo_candidatura, nr_processo, 
    cd_situacao_candidato_pleito, ds_situacao_candidato_pleito, cd_situacao_candidato_urna, 
    ds_situacao_candidato_urna, st_candidato_inserido_urna]; 
    console.log(value) 
    await pool.query('set datestyle = dmy');
    pool.query(quer, value)

}
run();
module.exports = {run};