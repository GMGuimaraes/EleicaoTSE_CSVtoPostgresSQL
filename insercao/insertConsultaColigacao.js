const fs = require('fs');
const csv = require('csv-parser'); 
const client = require('../db/connection');

async function run(){
  const nomeArquivos =  fs.readdirSync('./TabelasCSV/consulta_coligacao_2020');
  const arquivos = nomeArquivos.filter( (element) =>{
    return   element !== 'leiame.pdf' && element !== '.~lock.consulta_coligacao_2020_AM.csv#';
  })
  for (let index = 0; index < arquivos.length; index++) {
    const objs = []
      fs.createReadStream('./TabelasCSV/consulta_coligacao_2020/'+arquivos[index])
      .pipe(csv({
          separator: ';',
          mapHeaders: ({ header, index }) => header.toLowerCase(),
          
        }))
        .on('data',  (data) => inserir(data))
  }
}
async function inserir(obj){
  

  const query = `
  INSERT INTO eleicao.consulta_coligacao(
    ano_eleicao, cd_tipo_eleicao, nm_eleicao, nr_turno, 
    cd_eleicao, ds_eleicao, dt_eleicao, sg_uf, sg_ue, nm_ue, 
    cd_cargo, ds_cargo, tp_agremiacao, sq_coligacao, nm_coligacao, 
    ds_composicao_coligacao, cd_situacao_legenda, ds_situacao)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18);
  `
  const {
    ano_eleicao, cd_tipo_eleicao, nm_eleicao, nr_turno, 
    cd_eleicao, ds_eleicao, dt_eleicao, sg_uf, sg_ue, nm_ue, 
    cd_cargo, ds_cargo, tp_agremiacao, sq_coligacao, nm_coligacao, 
    ds_composicao_coligacao, cd_situacao_legenda, ds_situacao
  } = obj;
  const value = [ano_eleicao, cd_tipo_eleicao, nm_eleicao, nr_turno, 
  cd_eleicao, ds_eleicao, dt_eleicao, sg_uf, sg_ue, nm_ue, 
  cd_cargo, ds_cargo, tp_agremiacao, sq_coligacao, nm_coligacao, 
  ds_composicao_coligacao, cd_situacao_legenda, ds_situacao]
  await client.query('set datestyle = dmy');
  await client.query(query, value);
}
run();
module.exports = {run};