from dagster import job, op, AssetMaterialization, Output, Out, In
from azure.storage.blob import BlobServiceClient

import great_expectations as ge
import pandas as pd
import numpy as np
import warnings
import openpyxl
import json

# Suprimir todos os warnings
warnings.filterwarnings("ignore")

# Funcao para carregar os dados
@op
def load_data():
    # url = "https://raw.githubusercontent.com/Gabrielchess/neoWay_Chalange/main/indicadoresCovid.csv"
    # raw_data = pd.read_csv(url)
    raw_data = pd.read_csv("indicadoresCovid.csv")

    for column in ["dataNotificacao", "dataInicioSintomas", "dataPrimeiraDose", "dataSegundaDose"]:
        raw_data[column+"Aux"] = (
            raw_data[column]
            .str.replace("-", "", regex=False)
            .fillna(0)
            .astype(int)
        )
    
    print(raw_data)
    return raw_data

@op(
    ins={"raw_data": In()},
    out={"validation_results": Out()}
)
@op(out={"validation_results": Out(is_required=True)})
def validate_data(raw_data: pd.DataFrame):
    print("Starting validation...")
    
    context = ge.get_context()
    data_source = context.data_sources.add_pandas("pandas")
    data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")
    
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": raw_data})
    
    # Updated expectations based on criteria
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
            column="dataPrimeiraDose", meta={"dimensao": "Preenchimento"}
        ),
        ge.expectations.ExpectColumnValuesToNotBeNull(
            column="dataSegundaDose", meta={"dimensao": "Preenchimento"}
        ),
        ge.expectations.ExpectColumnValuesToNotBeNull(
            column="uf", meta={"dimensao": "Preenchimento"}
        ),
        ge.expectations.ExpectColumnValuesToNotBeNull(
            column="idade", meta={"dimensao": "Preenchimento"}
        ),

        # source_id validation
        ge.expectations.ExpectColumnValuesToBeUnique(
            column="source_id", 
            mostly=0.9,
            meta={"dimensao": "Unicidade"}
        ),
        
        ge.expectations.ExpectColumnValuesToMatchStrftimeFormat(
            column="dataNotificacao", 
            strftime_format="%Y-%m-%d", 
            meta={"dimensao": "Padronizacao"}
        ),
        
        ge.expectations.ExpectColumnValuesToMatchStrftimeFormat(
            column="dataInicioSintomas", 
            strftime_format="%Y-%m-%d", 
            meta={"dimensao": "Padronizacao"}
        ),
        
        # Text format validations
        # Não deve possuir números, caracteres especiais, acentuação e deve ser formatado em letras maiúsculas
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="sintomas", 
            regex=r'^[A-Z\s]*$', 
            meta={"dimensao": "Padronizacao"}
        ),
        
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="profissionalSaude", 
            regex="^(SIM|NAO)$", 
            meta={"dimensao": "Padronizacao"}
        ),
        
        ge.expectations.ExpectColumnValuesToBeInSet(
            column="racaCor", 
            value_set=["AMARELA", "BRANCA", "IGNORADO", "INDIGENA", "PARDA", "PRETA"], 
            meta={"dimensao": "Padronizacao"}
        ),
        
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="outrosSintomas", 
            regex=r'^[A-Z\s]*$', 
            meta={"dimensao": "Padronizacao"}
        ),
        
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="sexo", 
            regex="^(MASCULINO|FEMININO|INDEFINIDO)$", 
            meta={"dimensao": "Padronizacao"}
        ),
        
        ge.expectations.ExpectColumnValuesToBeInSet(
            column="uf", 
            value_set= [
                'AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO',
                'AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI',
                'RN', 'SE', 'DF', 'GO', 'MT', 'MS', 'ES',
                'MG', 'RJ', 'SP', 'PR', 'RS', 'SC'],

            meta={"dimensao": "Abragencia"}
        ),

        # Não deve possuir números, caracteres especiais, acentuação e deve ser formatado em letras maiúsculas        
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="municipio", 
            regex=r'^[A-Z\s]*$', 
            meta={"dimensao": "Padronizacao"}
        ),
        
        # Vaccine dates validations
        ge.expectations.ExpectColumnValuesToBeBetween(
            column="dataPrimeiraDoseAux", 
            min_value=20200323, 
            max_value=20220722, 
            meta={"dimensao": "Consistencia"}
        ),
        ge.expectations.ExpectColumnValuesToMatchStrftimeFormat(
            column="dataPrimeiraDose", 
            strftime_format="%Y-%m-%d", 
            meta={"dimensao": "Padronizacao"}
        ),
        
        ge.expectations.ExpectColumnValuesToBeBetween(
            column="dataNotificacaoAux", 
            min_value=20200104, 
            max_value=20220722, 
            meta={"dimensao": "Consistencia"}
        ),
        ge.expectations.ExpectColumnValuesToBeBetween(
            column="dataSegundaDoseAux", 
            min_value=20200819, 
            max_value=20220722, 
            meta={"dimensao": "Consistencia"}
        ),
        ge.expectations.ExpectColumnValuesToBeBetween(
            column="dataInicioSintomasAux", 
            min_value=20200104, 
            max_value=20220722, 
            meta={"dimensao": "Consistencia"}
        ),
        ge.expectations.ExpectColumnValuesToMatchStrftimeFormat(
            column="dataSegundaDose", 
            strftime_format="%Y-%m-%d", 
            meta={"dimensao": "Padronizacao"}
        ),
        
        # Vaccine manufacturer validations
        ge.expectations.ExpectColumnValuesToBeInSet(
            column="codigoLaboratorioPrimeiraDose", 
            value_set=["ASTRAZENECA/FIOCRUZ", "JANSSEN", "SINOVAC/BUTANTAN", "PFIZER"], 
            meta={"dimensao": "Padronizacao"}
        ),
        
        ge.expectations.ExpectColumnValuesToBeInSet(
            column="codigoLaboratorioSegundaDose", 
            value_set=["ASTRAZENECA/FIOCRUZ", "JANSSEN", "SINOVAC/BUTANTAN", "PFIZER"], 
            meta={"dimensao": "Padronizacao"}
        ),
        
        # Age validation
        ge.expectations.ExpectColumnValuesToMatchRegex(
            column="idade", 
            regex=r'^-?\d+(\.\d+)?$', 
            meta={"dimensao": "Padronizacao"}
        ),
        # ge.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        #     column_A="dataSegundaDoseAux",
        #     column_B="dataPrimeiraDoseAux",
        #     or_equal=False,
        #     meta={"dimensao": "Consistencia"}
        # ),

        ge.expectations.ExpectColumnValuesToBeBetween(
            column="idade", 
            min_value=0, 
            max_value=122, 
            strict_max=True, 
            meta={"dimensao": "Consistencia"}
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
                "unexpected_values": unexpected_values
                # "exception_events": {
                #     "exception_message": str(result.exception_info.message) if not result.success and result.exception_info else None,
                #     "exception_traceback": str(result.exception_info.traceback) if not result.success and result.exception_info else None
                # }
            }
            
            validation_results.append(validation_result)
            
            # Yield asset materialization
            yield AssetMaterialization(
                asset_key=f"validation_{expectation.column}",
                description=f"Validation result for column {expectation.column}",
                metadata={
                    "type": str(expectation_type),
                    "success": str(result.success),
                    #"element_count": int(element_count),
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
        # Itera sobre cada resultado
        for result in validation_results:
            # Remove o campo 'unexpected_values', se existir
            unexpected_values = result.pop('unexpected_values', None)
            
            # Convert any 'NaN' to None (which becomes null in JSON)
            for result in validation_results:
                if 'unexpected_values' in result:
                    result['unexpected_values'] = [
                        np.null if value == np.NaN else value 
                        for value in result['unexpected_values']
                        ]

            # Adiciona o campo novamente ao dicionário
            result['unexpected_values'] = [
                None if value is np.NaN else value  # Substitui NaN por None
                for value in (unexpected_values or [])  # Garante que seja uma lista válida
            ]
        
        # Salva os resultados no arquivo JSON
        with open('validation_results.json', 'w', encoding='utf-8') as f:
            json.dump(validation_results, f, indent=4, ensure_ascii=False)
        
        return 'validation_results.json'
    except Exception as e:
        print(f"Error generating report: {str(e)}")
        return None

@op(
    ins={
        "raw_data": In(),
        "validation_results": In()
    },
    out={"excel_path": Out()}
)

@op
def generate_excel_report(raw_data: pd.DataFrame, validation_results: list):
    # Criar uma cópia do DataFrame original
    df_with_validations = raw_data.copy()
    
    # Criar colunas de validação para cada coluna original
    for result in validation_results:
        column = result.get('column')
        if column == 'unknown' or not column:
            continue
            
        # Criar nome da coluna de validação
        validation_column = f"{column}_validation"
        
        # Inicializar todas as linhas como 'VÁLIDO'
        df_with_validations[validation_column] = 'VÁLIDO'
        
        # Se houver valores inesperados, marcar como 'INVÁLIDO'
        if not result.get('success'):
            unexpected_values = result.get('unexpected_values', [])
            
            # Converter valores inesperados para string para comparação segura
            unexpected_values = [str(val) if val is not None else '' for val in unexpected_values]
            
            # Marcar valores inválidos
            mask = df_with_validations[column].astype(str).isin(unexpected_values)
            df_with_validations.loc[mask, validation_column] = 'INVÁLIDO'
            
            # Adicionar informação do tipo de validação que falhou
            df_with_validations.loc[mask, f"{column}_failure_type"] = result.get('type')
            
    # Salvar para Excel
    excel_path = 'raw_data_with_validations.xlsx'
    df_with_validations.to_excel(excel_path, index=False)
    
    return excel_path

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
    
    # Gerar ambos os relatórios
    file_path = generate_report(validation_results)
    excel_path = generate_excel_report(raw_data, validation_results)
    
    # Upload dos arquivos
    json_blob_url = upload_to_azure(file_path)
    excel_blob_url = upload_to_azure(excel_path)  # Você pode querer criar uma nova operação específica para o Excel
    
    finalize_pipeline(json_blob_url)  # Modificar para incluir ambas URLs se necessário