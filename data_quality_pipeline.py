from dagster import job, op, AssetMaterialization, Output, Out, In
from azure.storage.blob import BlobServiceClient

import great_expectations as ge
import pandas as pd
import numpy as np
import warnings
import json

# Suprimir todos os warnings
warnings.filterwarnings("ignore")

# Funcao para carregar os dados
@op
def load_data():
    url = "https://raw.githubusercontent.com/Gabrielchess/neoWay_Chalange/main/indicadoresCovid.csv"
    raw_data = pd.read_csv(url)

    return raw_data

@op(
    ins={"raw_data": In()},
    out={"validation_results": Out()}
)
@op(out={"validation_results": Out(is_required=True)})  # Explicitly define the output
def validate_data(raw_data: pd.DataFrame):
    print("Starting validation...")  # Debug print
    
    # Criando o contexto de dados do Great Expectations
    context = ge.get_context()
    data_source = context.data_sources.add_pandas("pandas")
    data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")
    
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": raw_data})
    
    expectations = [
        # Dimensão 1: Preenchimento
        ge.expectations.ExpectColumnValuesToNotBeNull(
            column="source_id", meta={"dimensao": "Preenchimento"}
        ),

        ge.expectations.ExpectColumnValuesToNotBeNull(
            column="dataNotificacao", meta={"dimensao": "Preenchimento"}
        ),
        ge.expectations.ExpectColumnValuesToNotBeNull(
            column="dataInicioSintomas", meta={"dimensao": "Preenchimento"}
        ),
        ge.expectations.ExpectColumnValuesToNotBeNull(
            column="uf", meta={"dimensao": "Preenchimento"}
        ),
        ge.expectations.ExpectColumnValuesToNotBeNull(
            column="idade", meta={"dimensao": "Preenchimento"}
        ),
        
        # Dimensão 2: Padronizacao
        ge.expectations.ExpectColumnValuesToMatchStrftimeFormat(
            column="dataInicioSintomas", strftime_format="%Y-%m-%d", meta={"dimensao": "Padronizacao"}
        ),
        ge.expectations.ExpectColumnValuesToMatchStrftimeFormat(
            column="dataNotificacao", strftime_format="%Y-%m-%d", meta={"dimensao": "Padronizacao"}
        ),
        ge.expectations.ExpectColumnValuesToBeInSet(
            column="sexo", value_set=["Masculino", "Femenino", "Indefinido"], meta={"dimensao": "Padronizacao"}
        ),
        ge.expectations.ExpectColumnValuesToMatchStrftimeFormat(
            column="dataPrimeiraDose", strftime_format="%Y-%m-%d", mostly=1.0, meta={"dimensao": "Padronizacao"}
        ),
        ge.expectations.ExpectColumnValuesToMatchStrftimeFormat(
            column="dataSegundaDose", strftime_format="%Y-%m-%d", mostly=1.0, meta={"dimensao": "Padronizacao"}
        ),
        ge.expectations.ExpectColumnValuesToBeInSet(
            column="racaCor", value_set=["AMARELA", "BRANCA", "IGNORADO", "INDIGENA", "PARDA", "PRETA"], meta={"dimensao": "Padronizacao"}
        ),
        ge.expectations.ExpectColumnValuesToBeInSet(
            column="sexo", value_set=["Masculino", "Feminino", "Indefinido"], meta={"dimensao": "Padronizacao"}
        ),        
        ge.expectations.ExpectColumnValuesToBeInSet(
            column="codigoLaboratorioSegundaDose", 
            value_set= ["ASTRAZENECA/FIOCRUZ", "JANSSEN", "SINOVAC/BUTANTAN", "PFIZER"],
            mostly=1.0, meta={"dimensao": "Padronizacao"}
        ),
        
        # Colunas com valores específicos em LETRAS MAIÚSCULAS
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="profissionalSaude", regex="^(Sim|Nao)$", meta={"dimensao": "Padronizacao"}
        ),
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="sintomas", regex=r'^[A-Z\s]*$', mostly=1.0, meta={"dimensao": "Padronizacao"}
        ),
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="outrosSintomas", regex=r'^[A-Z\s]*$', mostly=1.0, meta={"dimensao": "Padronizacao"}
        ),
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="municipio", regex=r'^[A-Z\s]*$', mostly=1.0, meta={"dimensao": "Padronizacao"}
        ),
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="idade", regex=r'^\d+$', meta={"dimensao": "Padronizacao"}
        ),
        
        # Dimensão 3: Consistencia
        ge.expectations.ExpectColumnValuesToBeBetween(
            column="dataNotificacao", min_value="2020-01-04", max_value="2022-07-22", meta={"dimensao": "Consistencia"}
        ),
        ge.expectations.ExpectColumnValuesToBeBetween(
            column="dataInicioSintomas", min_value="2020-01-04", max_value="2022-07-22", meta={"dimensao": "Consistencia"}
        ),
        ge.expectations.ExpectColumnValuesToBeBetween(
            column="dataPrimeiraDose", min_value="2020-03-23", max_value="2022-07-22", mostly=1.0, meta={"dimensao": "Consistencia"}
        ),
        ge.expectations.ExpectColumnValuesToBeBetween(
            column="dataSegundaDose", min_value="2020-08-19", max_value="2022-07-22", mostly=1.0, meta={"dimensao": "Consistencia"}
        ),
        ge.expectations.ExpectColumnValuesToBeBetween(
            column="idade", min_value=0, max_value=122, strict_max=True, meta={"dimensao": "Consistencia"}
        ),
        # ge.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        #     column_A="dataPrimeiraDose", column_B="dataSegundaDose", mostly=1.0, meta={"dimensao": "Consistencia"}
        # ),
        
        # Dimensão 4: Unicidade
        ge.expectations.ExpectColumnValuesToBeUnique(
            column="source_id", mostly=0.9, meta={"dimensao": "Unicidade"}
        ),
        
        # Dimensão 5: Abrangencia
        ge.expectations.ExpectColumnValuesToBeInSet(
            column="uf", value_set=[
                "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
                "MA", "MT", "MS", "MG","PA", "PB", "PR", "PE", "PI",
                "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"], meta={"dimensao": "Abrangencia"}
        )
    ]
    
    validation_results = []
    
    print(f"Processing {len(expectations)} expectations...")  # Debug print
    
    for idx, expectation in enumerate(expectations, 1):
        try:
            result = batch.validate(expectation)
            
            # Extract values safely with explicit default values
            unexpected_values = result.result.get("partial_unexpected_list", [])
            element_count = result.result.get("element_count", 0)
            failed_events = result.result.get("unexpected_count", 0)
            failed_percentage = result.result.get("unexpected_percent", 0.0)
            
            expectation_type = result.expectation_config.get("type", "unknown")
            dimensao = result.expectation_config.get("meta", {}).get("dimensao", "unknown")
            
            # Clean up any numpy or non-serializable types
            unexpected_values = [
                str(x) if isinstance(x, (np.generic, pd.Timestamp)) 
                else None if pd.isna(x) 
                else x 
                for x in unexpected_values
            ]
            
            validation_result = {
                "type": str(expectation_type),
                "column": str(getattr(expectation, 'column', 'unknown')),
                "dimensao": dimensao,
                "success": bool(result.success),
                "element_count": int(element_count),
                "failed_count": int(failed_events),
                "failed_percentage": float(failed_percentage),
                "details": str(result.exception_info) if not result.success else "No issues",
                "unexpected_values": unexpected_values
            }
            
            validation_results.append(validation_result)
            
            # Yield asset materialization
            yield AssetMaterialization(
                asset_key=f"validation_{expectation.column}",
                description=f"Validation result for column {expectation.column}",
                metadata={
                    "type": str(expectation_type),
                    "success": str(result.success),
                    "failed_count": str(failed_events)
                }
            )
            
            print(f"Processed expectation {idx}/{len(expectations)}")  # Debug print
            
        except Exception as e:
            print(f"Error processing expectation {idx}: {str(e)}")  # Debug print
            continue
    
    print(f"Completed validation with {len(validation_results)} results")  # Debug print
    
    # Make sure validation_results is not empty
    if not validation_results:
        validation_results = [{"error": "No validation results generated"}]
    
    # Final output
    yield Output(
        value=validation_results,
        output_name="validation_results"
    )

@op(
    ins={"validation_results": In()},
    out={"file_path": Out()}
)
@op
def generate_report(validation_results):
    try:
        with open('validation_results.json', 'w') as f:
            json.dump(validation_results, f, indent=4)
        return 'validation_results.json'
    except Exception as e:
        print(f"Error generating report: {str(e)}")
        return None
    
@op(
    ins={"file_path": In()},
    out={"blob_url": Out()}
)
def upload_to_azure(file_path: str):
    if not file_path:
        raise ValueError("No file path provided for upload")
        
    connection_string = "DefaultEndpointsProtocol=https;AccountName=validationneoway;AccountKey=f/Lzl525PRQ7U9NoosQ5nBHZv6tRJYgFKp4IHQOlgZzoc90SXxEOvlMPES3E9sfUAGrFtH4LYjT9+AStMblCLA==;EndpointSuffix=core.windows.net"
    container_name = "variaveisvalidacao"
    blob_name = "validation_results.json"

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        with open(file_path, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)

        blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}"
        return blob_url
    except Exception as e:
        print(f"Error uploading to Azure: {str(e)}")
        return None
    
# Função para exibir a URL no relatório final
@op(ins={"blob_url": In()})
def finalize_pipeline(blob_url: str):
    print(f"O arquivo foi carregado com sucesso no Azure Blob Storage.")
    print(f"URL do arquivo: {blob_url}")
    return blob_url

@job
def data_pipeline():
    raw_data = load_data()
    validation_results = validate_data(raw_data)
    file_path = generate_report(validation_results)  # Get the file path from generate_report
    blob_url = upload_to_azure(file_path)  # Pass the file path to upload_to_azure
    finalize_pipeline(blob_url)