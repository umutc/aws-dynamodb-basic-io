const readFileSync = require('fs').readFileSync;
const AWS = require('aws-sdk');
AWS.config.update({ region: 'eu-west-1' });
const DynamoDB = new AWS.DynamoDB();
const TABLE_NAME = 'PetInventory'
function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

async function init() {
    try {
        // Create table if not exist
        const tables = await DynamoDB.listTables({ Limit: 10 }).promise();
        if (!tables.TableNames.includes(TABLE_NAME)) {
            const dataCreationResult = await DynamoDB.createTable({
                TableName: TABLE_NAME, AttributeDefinitions: [
                    {
                        AttributeName: 'pet_id',
                        AttributeType: 'N'
                    }
                ],
                KeySchema: [

                    {
                        AttributeName: 'pet_id',
                        KeyType: 'HASH'
                    }
                ],
                ProvisionedThroughput: {
                    ReadCapacityUnits: 5,
                    WriteCapacityUnits: 5
                },
            }).promise();
            console.log(dataCreationResult);
            await sleep(10000);
        }


        const dataCsv = readFileSync('./sample-data.csv', { encoding: "utf8" });
        const dataDynamoDBJson = dataCsv
            .split('\n')
            .map(row => row.split(','))
            .map(item => ({
                "age": {
                    "N": item[4]
                },
                "color": {
                    "S": item[3]
                },
                "gender": {
                    "S": item[5]
                },
                "name": {
                    "S": item[2]
                },
                "pet_id": {
                    "N": item[1]
                },
                "pet_species": {
                    "S": item[0]
                },
                // Added new property
                "pet_available": {
                    "S": item[6]
                },
                "scale_texture": {
                    "S": item[7]
                }
            }));
        const params = {
            RequestItems: {
                'PetInventory': dataDynamoDBJson.map(item => ({
                    PutRequest: {
                        Item: item
                    }
                }))
            }
        };
        await DynamoDB.batchWriteItem(params).promise();
        const scannedItems = await DynamoDB.scan({
            TableName: TABLE_NAME
        }).promise();
        console.log(scannedItems.Items);
    } catch (error) {
        console.log(error);
    }
}

init();