const fs = require('fs');
const csv = require('csv-parser'); 
const client = require('../db/connection');

async function run(){
  const nomeArquivos =  fs.readdirSync('./TabelasCSV/consulta_vagas_2020');
  const arquivos = nomeArquivos.filter( (element) =>{
    return   element !== 'leiame.pdf';
  })
  for (let index = 0; index < arquivos.length; index++) {
      fs.createReadStream('./TabelasCSV/consulta_vagas_2020/'+arquivos[index])
      .pipe(csv({
          separator: ';',
          mapHeaders: ({ header, index }) => header.toLowerCase(),
          
        }))
        .on('data',  (data) => inserir(data))
        //.on('end', () =>inserir(objs));

    
    
  }
}
async function inserir(obj){
  const query = `
  INSERT INTO eleicao.consulta_vagas(
    ano_eleicao, cd_tipo_eleicao, nm_tipo_eleicao, cd_eleicao, ds_eleicao, 
    dt_eleicao, dt_posse, sg_uf, sg_ue, nm_ue, cg_cargo, ds_cargo, 
    qt_vagas)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
  `
  const {
    ano_eleicao, cd_tipo_eleicao, nm_tipo_eleicao, cd_eleicao, ds_eleicao, dt_eleicao, dt_posse, sg_uf, 
    sg_ue, nm_ue, cg_cargo, ds_cargo, qt_vagas
  } = obj;
  const value = [ ano_eleicao, cd_tipo_eleicao, nm_tipo_eleicao, cd_eleicao, ds_eleicao, dt_eleicao, dt_posse, sg_uf, 
    sg_ue, nm_ue, cg_cargo, ds_cargo, qt_vagas]
  await client.query('set datestyle = dmy');
  await client.query(query, value);
  
}
run();
module.exports = {run};