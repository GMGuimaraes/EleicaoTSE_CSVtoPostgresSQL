const fs = require('fs');
const csv = require('csv-parser'); 
const pool = require('../db/connection');
let part = []
async function run(){
  const nomeArquivos =  fs.readdirSync('./TabelasCSV/consulta_coligacao_2020');
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
          const {nr_partido} = data;
          
            if(part.includes(nr_partido)){
              
            }else{
              part.push(nr_partido)
              inserir(data)

            }
        })
  }
}

 function inserir(obj){

  pool.connect()
  const quer = `
  INSERT INTO eleicao.partidos(
    nr_partindo,sg_partido,
    nm_partido,sq_coligacao)
    VALUES ($1, $2, $3, $4);
  `
  const {
    nr_partido,sg_partido,
    nm_partido,sq_coligacao
  } = obj;
 
  const value = [nr_partido,sg_partido,
    nm_partido,sq_coligacao]; 
        pool.query(quer, value)
  
}
run();
module.exports = {run};