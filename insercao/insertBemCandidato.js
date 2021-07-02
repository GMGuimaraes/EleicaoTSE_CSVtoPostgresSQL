const fs = require('fs');
const csv = require('csv-parser'); 
const pool = require('../db/connection');
let part = []

async function run(){
  
  const nomeArquivos =  fs.readdirSync('./TabelasCSV/bem_candidato_2020');
  const arquivos = nomeArquivos.filter( (element) =>{
    return   element !== 'leiame.pdf';
  })
  for (let index = 0; index < arquivos.length; index++) {
      fs.createReadStream('./TabelasCSV/consulta_coligacao_2020/'+arquivos[index])
      .pipe(csv({
          separator: ';',
          mapHeaders: ({ header, index }) => header.toLowerCase(),
          
        }))
        .on('data',  (data)   =>  {
          const {sq_candidato} = data;
          
            if(part.includes(sq_candidato)){
              
            }else{
              part.push(sq_candidato)
              inserir(data)

            }
        })
  }
}

 function inserir(obj){

  pool.connect()
  const quer = `
  INSERT INTO eleicao.bem_candidato(
    sg_uf, sg_ue, nm_ue, 
    sq_candidato, nr_ordem_candidato, 
    cd_tipo_bem_candidato, ds_tipo_bem_candidato, 
    ds_bem_candidato, vr_bem_candidato, dt_ultima_atualizacao, 
    hh_ultima_atualizacao, id_bem_candidato)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
  `
  const {
    sg_uf, sg_ue, nm_ue, 
    sq_candidato, nr_ordem_candidato, 
    cd_tipo_bem_candidato, ds_tipo_bem_candidato, 
    ds_bem_candidato, vr_bem_candidato, dt_ultima_atualizacao, 
    hh_ultima_atualizacao, id_bem_candidato
  } = obj;
 
  const value = [sg_uf, sg_ue, nm_ue, 
    sq_candidato, nr_ordem_candidato, 
    cd_tipo_bem_candidato, ds_tipo_bem_candidato, 
    ds_bem_candidato, vr_bem_candidato, dt_ultima_atualizacao, 
    hh_ultima_atualizacao, id_bem_candidato]; 
    console.log(value) 
     
    pool.query(quer, value)
  
}
run();
module.exports = {run};