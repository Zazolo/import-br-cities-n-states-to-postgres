const fs = require('fs');
const axios = require('axios');
const csv = require('csv-parser');
const { Pool } = require('pg');
const { randomUUID } = require('crypto');
const iconv = require('iconv-lite');

// Configuração do banco de dados PostgreSQL
const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'lanxe-api',
    password: '1234',
    port: 5432,
});

const STATES_TABLE_NAME = 'state';
const CITIES_TABLE_NAME = 'city';

// URLs dos arquivos CSV
const csvUrls = [
    'https://blog.mds.gov.br/redesuas/wp-content/uploads/2018/06/Lista_Estados_Brasil_Versao_CSV.csv',
    'https://www.gov.br/receitafederal/dados/municipios.csv/@@download/file',
];

// Função para fazer download de um arquivo
const downloadCsv = async (url, outputFilePath) => {
    const response = await axios({
        url,
        method: 'GET',
        responseType: 'stream',
    });

    return new Promise((resolve, reject) => {
        const writer = fs.createWriteStream(outputFilePath);
        response.data.pipe(writer);
        writer.on('finish', resolve);
        writer.on('error', reject);
    });
};

// Função para inserir dados no PostgreSQL
const insertStatesIntoPostgres = async (data) => {
    const { id, name, ibgeCode, abbreviation } = data;

    try {
      // Verifica se já existe um registro com a mesma abbreviation
      const checkQuery = 'SELECT id FROM '+ STATES_TABLE_NAME +' WHERE abbreviation = $1';
      const checkResult = await pool.query(checkQuery, [abbreviation]);

      // Se o registro já existir, não realiza a inserção
      if (checkResult.rows.length > 0) {
        console.log(`Registro com abbreviation ${abbreviation} já existe. Pulando inserção.`);
        return checkResult.rows[0].id;
      }

      // Se não existir, insere o novo registro
      const insertQuery = 'INSERT INTO '+ STATES_TABLE_NAME +' ("id", "ibgeCode", "name", "abbreviation") VALUES ($1, $2, $3, $4) RETURNING id';
      console.log('Inserindo novo registro:', insertQuery);
      const insertResult = await pool.query(insertQuery, [id, ibgeCode, name, abbreviation]);

      return insertResult.rows[0].id;
    } catch (error) {
      console.error('Erro ao inserir ou verificar dados no PostgreSQL:', error);
      throw error;
    }
  };
const insertCitiesIntoPostgres = async (data) => {
    const { id, name, ibgeCode, stateId } = data;

    try {
      // Verifica se já existe um registro com a mesma abbreviation
      const checkQuery = 'SELECT id FROM '+ CITIES_TABLE_NAME +' WHERE name = $1';
      const checkResult = await pool.query(checkQuery, [name]);

      // Se o registro já existir, não realiza a inserção
      if (checkResult.rows.length > 0) {
        console.log(`Registro com abbreviation ${abbreviation} já existe. Pulando inserção.`);
        return null;
      }

      // Se não existir, insere o novo registro
      const insertQuery = 'INSERT INTO '+ CITIES_TABLE_NAME +' ("id", "ibgeCode", "name", "stateId") VALUES ($1, $2, $3, $4) RETURNING id';
      console.log('Inserindo novo registro:', insertQuery);
      const insertResult = await pool.query(insertQuery, [id, ibgeCode, name, stateId]);

      return insertResult.rows[0].id;
    } catch (error) {
      //console.error('Erro ao inserir ou verificar dados no PostgreSQL:', error);
      throw error;
    }
  };

// Função para ler CSV como stream e salvar no banco
const processCsv1 = async (filePath) => {
    const savedObjects = [];
    const insertPromises = []; // Armazena as promessas de inserção

    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(iconv.decodeStream('latin1'))
            .pipe(csv({ separator: ';' }))
            .on('data', (row) => {
                const value = {
                    id: randomUUID(),
                    ibgeCode: row['IBGE'],
                    name: row['Estado'],
                    abbreviation: row['UF'],
                };

                // Armazena a promessa de inserção no array
                const insertPromise = insertStatesIntoPostgres(value)
                    .then((id) => {
                        if (id) {
                            value.id = id;
                            savedObjects.push(value);
                        }
                    })
                    .catch((error) => {
                        console.error('Erro ao inserir dados:', error);
                    });

                insertPromises.push(insertPromise);
            })
            .on('end', async () => {
                // Aguarda que todas as inserções tenham sido processadas
                await Promise.all(insertPromises);
                console.log("Resolvendo::", savedObjects);
                resolve(savedObjects);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
};


const processCsv2 = async (filePath, stateObjects) => {
    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(iconv.decodeStream('latin1'))
            .pipe(csv({separator: ';'}))
            .on('data', async (row) => {
                try {
                    const value = {
                        id: randomUUID(),
                        stateId: stateObjects.find(state => state.abbreviation === row['UF']).id,
                        ibgeCode: row['CÓDIGO DO MUNICÍPIO - IBGE'],
                        name: row['MUNICÍPIO - IBGE'] || row['MUNICÍPIO - TOM'],
                        abbreviation: row['UF'],
                    }
                    await insertCitiesIntoPostgres(value);
                } catch (error) {
                    console.error('Erro ao inserir dados:', error);
                }
            })
            .on('end', () => {
                resolve();
            })
            .on('error', (error) => {
                reject(error);
            });
    });
};

// Função principal
const main = async () => {
    try {
        // Faz download dos dois arquivos CSV
        await Promise.all([
            downloadCsv(csvUrls[0], './file1.csv'),
            downloadCsv(csvUrls[1], './file2.csv'),
        ]);

        console.log('Arquivos CSV baixados com sucesso.');

        // Processa o arquivo desejado (substitua por file1.csv ou file2.csv)
        const savedObjects = await processCsv1('./file1.csv');
        await processCsv2('./file2.csv', savedObjects);

        console.log('Operação concluída');
    } catch (error) {
        console.error('Erro no processo:', error);
    } finally {
        //await pool.end();
    }
};

main();
