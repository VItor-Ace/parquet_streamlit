import string
import random
import streamlit_authenticator as stauth
import streamlit as st
import pandas as pd
from io import BytesIO
import os
from datetime import datetime
import boto3
import yaml
from yaml.loader import SafeLoader

# ---------------------------- WORKING WITH THE FILES STREAMLIT AND AWS SERVICES ------------------------------------- #
s3 = boto3.client(
    's3',
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    region_name=st.secrets["AWS_REGION"]
)

bucket = st.secrets["BUCKET_NAME"]
key = st.secrets["PARQUET_KEY"]

# ---------------------------------------- CREATING AUTHENTICATION PROCESS ------------------------------------------- #

config = {
    'credentials': {
        'usernames': {
            username: {
                'email': st.secrets["credentials"]["usernames"][username]["email"],
                'name': st.secrets["credentials"]["usernames"][username]["name"],
                'password': st.secrets["credentials"]["passwords"][username]
            } for username in st.secrets["credentials"]["usernames"]
        }
    },
    'cookie': {
        'name': st.secrets["cookie"]["name"],
        'key': st.secrets["cookie"]["key"],
        'expiry_days': st.secrets["cookie"]["expiry_days"],
    },
    'preauthorized': {
        'emails': st.secrets["preauthorized"]["emails"]
    }
}

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']["emails"]
)

st.title("Editor e Visualizador de Tabela")

auth_container = st.container()

with auth_container:
    if not st.session_state.get('authentication_status'):
        tabs = st.tabs(["Login", "Registrar", "Esqueci a senha", "Esqueci o usuário"])

        with tabs[0]:
            try:
                authenticator.login(
                    "main",
                    1,
                    4,
                    {'Form name': 'Login', 'Username': 'Nome de usuário', 'Password': 'Senha', 'Login': 'Login',
                     'Captcha': 'Captcha'},
                    True
                )

            except Exception as e:
                st.error(e)

        with tabs[1]:
            st.subheader("Novo registro de usuário.")

            try:
                email_of_registered_user, \
                    username_of_registered_user, \
                    name_of_registered_user = authenticator.register_user(
                    fields={'Form name': 'Registro', 'Email': 'Email', 'Username': 'Nome de usuário',
                            'Password': 'Senha', 'Repeat password': 'Repita a senha',
                            'Password hint': 'Dica para a senha', 'Captcha': 'Captcha', 'Register': 'Registrar'})
                if email_of_registered_user:
                    st.success('Usuário registrado com sucesso!')
                    with open('credentials.yaml', 'w', encoding='utf-8') as file:
                        yaml.dump(authenticator.credentials, file, default_flow_style=False, allow_unicode=True)

            except Exception as e:
                st.error(f"Erro ao registrar: {e}")

        with tabs[2]:
            st.subheader("Recuperar senha")
            try:
                username_of_forgotten_password, \
                    email_of_forgotten_password, \
                    new_random_password = authenticator.forgot_password(
                    fields={'Form name': 'Esqueci minha senha', 'Username': 'Nome de usuário', 'Captcha': 'Captcha',
                            'Submit': 'Feito!'})
                if username_of_forgotten_password:
                    st.success('Nova senha enviada com segurança!')
                    # To securely transfer the new password to the user please see step 8.
                    # Salvar as alterações no arquivo
                    with open('credentials.yaml', 'w', encoding='utf-8') as file:
                        yaml.dump(authenticator.credentials, file, default_flow_style=False, allow_unicode=True)
                else:
                    st.error('Usúario não encontrado.')

            except Exception as e:
                st.error(e)

        with tabs[3]:
            st.subheader("Recuperar nome de usuário")
            try:
                username_recovered = authenticator.forgot_username(
                    fields={'Form name': 'Esqueci meu usuário', 'Email': 'Email', 'Captcha': 'Captcha',
                            'Submit': 'Feito!'})
                if username_recovered:
                    st.success(f"Nome de usuário enviado para o email associado!")
            except Exception as e:
                st.error(e)

# ------------------------------------- DEFINING FUNCTIONS TO BE USED AFTER ------------------------------------------ #
    @st.cache_data
    def read_from_s3(bucket_f, key_f):
        """Read parquet file directly from S3"""
        response = s3.get_object(Bucket=bucket_f, Key=key_f)
        return pd.read_parquet(BytesIO(response['Body'].read()))


    def write_to_s3(df_f, bucket_f, key_f):
        """Write dataframe to S3 as parquet"""
        buffer = BytesIO()
        df_f.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.put_object(Bucket=bucket_f, Key=key_f, Body=buffer)
        return True

    def generating_random_code(length=3):
        characters = string.ascii_letters + string.digits
        code = ''.join(random.choice(characters) for _ in range(length))
        return code


    def update_credentials_file():
        """Must be called after ANY credential change"""
        with open('credentials.yaml', 'w', encoding='utf-8') as file_:
            yaml.dump(
                authenticator.credentials,
                file_,
                default_flow_style=False,  # Critical for proper YAML format
                allow_unicode=True,  # Preserves special characters
                sort_keys=False  # Maintains original order
            )

# ----------------------------------------- CREATING WEBAPP AFTER LOGGING -------------------------------------------- #

with st.sidebar:
    if st.session_state.get('authentication_status'):
        # Logout (permanece)
        authenticator.logout('Sair', 'sidebar')

if st.session_state.get('authentication_status'):
    # Limpa as abas de autenticação
    auth_container.empty()

    # ---------------------------------- WRITING THE WEB APP INTERFACE AND COMMANDS ---------------------------------- #

    col1, col2 = st.columns([1, 4])

    with col1:
        st.image('Logo_Minimal_webapp.png', width=130)
    with col2:
        st.title(f"Bem vindo {st.session_state.get('name')}!")

    # Sidebar: file selection or upload
    st.sidebar.header("Carregue dados")
    mode = st.sidebar.radio("Escolha modo de leitura:", ("Use arquivo S3", "Faça upload de um arquivo local"))

    # Load data based on selection
    try:
        if mode == "Use arquivo S3":
            df = read_from_s3(bucket, key)
            st.sidebar.success(f'Carregado de S3: s3://{bucket}/{key}')
        else:
            uploaded = st.sidebar.file_uploader("Faça upload de um arquivo Parquet", type=["parquet"])
            if uploaded is not None:
                df = pd.read_parquet(uploaded)
                st.sidebar.success("Arquivo carregado.")
            else:
                st.sidebar.info("Por favor, carregue um arquivo ou use um arquivo S3.")
                st.stop()
    except Exception as e:
        st.error(f"Erro carregando dados: {str(e)}")
        st.stop()

    df.columns = ["." if str(col).startswith('Unnamed') else col for col in df.columns]

    # 2. Agora renomeia as colunas "." duplicadas com asteriscos PROGRESSIVOS
    novos_nomes = []
    contador_pontos = 0

    for nome in df.columns:
        if nome == '.':
            novos_nomes.append(f'{contador_pontos}')
            contador_pontos += 1
        else:
            novos_nomes.append(nome)  # Mantém outros nomes intactos

    df.columns = novos_nomes

    data_file_values = df.values.tolist()

    def processar_datas(df):
        data = df.values.tolist()
        for i, row in enumerate(data):
            for j, val in enumerate(row):
                if pd.isna(val) or val == 'nan':
                    data[i][j] = '--'
                elif isinstance(val, datetime):
                    data[i][j] = val.strftime("%d/%m/%Y")
                elif isinstance(val, str):
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
                        try:
                            parsed_date = datetime.strptime(val, fmt).date()
                            data[i][j] = parsed_date.strftime("%d/%m/%Y")
                            break
                        except ValueError:
                            continue
        return pd.DataFrame(data, columns=df.columns)

    # Main editor function
    def main_editor(df_f: pd.DataFrame) -> pd.DataFrame:
        st.subheader("Edite a Tabela")

        # Initialize session state keys for tracking the dataframe and its state
        if 'original_df_hash' not in st.session_state:
            # Store a hash of the original DF on first load for comparison
            st.session_state.original_df_hash = hash(pd.util.hash_pandas_object(df_f).sum())
            st.session_state.last_verified_df = df_f.copy()
            st.session_state.verification_code = None
            st.session_state.verified = False
            st.session_state.pending_deletion = False

        try:
            # Try newer data_editor first
            edited_df = st.data_editor(
                df_f,
                use_container_width=True,
                num_rows="dynamic",
                key="data_editor"
            )
        except AttributeError:
            # Fallback to experimental editor
            edited_df = st.experimental_data_editor(
                df_f,
                use_container_width=True,
                num_rows="dynamic",
                key="data_editor"
            )

        # Calculate the current hash of the edited DF
        current_hash = hash(pd.util.hash_pandas_object(edited_df).sum())
        # Get the hash of the last known good/verified state
        last_verified_hash = hash(pd.util.hash_pandas_object(st.session_state.last_verified_df).sum())

        # Check if the user has made changes SINCE the last verification/load
        has_changes = current_hash != last_verified_hash

        # Only check for deletions if there are actual changes
        if has_changes:
            # Compare against the last verified state, not the original load
            original_keys = set(hash(tuple(row)) for row in st.session_state.last_verified_df.values)
            edited_keys = set(hash(tuple(row)) for row in edited_df.values)
            lines_removed = original_keys - edited_keys

            # If lines were removed, set the state to pending deletion and generate a code
            if lines_removed and not st.session_state.verified:
                st.session_state.pending_deletion = True
                if st.session_state.verification_code is None:
                    st.session_state.verification_code = generating_random_code()
                st.warning(f'Houve(ram) {len(lines_removed)} linha(s) removida(s). Confirme a ação:')

                # Show verification UI
                col1, col2 = st.columns([3, 1])
                with col1:
                    user_input = st.text_input(
                        f"Digite '{st.session_state.verification_code}' para confirmar",
                        key="verification_input"
                    )
                with col2:
                    st.write("")  # Spacer
                    if st.button("Confirmar", key="verify_button"):
                        if user_input == str(st.session_state.verification_code):
                            st.session_state.verified = True
                            st.session_state.pending_deletion = False
                            # Update the last verified state to the current edited state
                            st.session_state.last_verified_df = edited_df.copy()
                            st.success("Remoção confirmada!")
                            st.rerun()  # Rerun to clear the verification UI
                        else:
                            st.error("Código incorreto!")
                            st.session_state.verified = False

                # If a deletion is pending and not verified, return the last verified (good) state
                # This prevents the app from showing the deleted rows until they are confirmed
                if st.session_state.pending_deletion:
                    return st.session_state.last_verified_df

            # If changes were made but no lines were removed, or if the deletion was verified,
            # update the last verified state
            else:
                st.session_state.last_verified_df = edited_df.copy()
                st.session_state.verification_code = None
                st.session_state.verified = False
                st.session_state.pending_deletion = False

        # If there are no changes, ensure the verification state is reset
        else:
            st.session_state.verification_code = None
            st.session_state.verified = False
            st.session_state.pending_deletion = False

        # Return the edited dataframe (which might be the same as last_verified_df if a deletion is pending)
        return edited_df

    # Display and edit data
    edited_df = main_editor(df)

    edited_df = processar_datas(edited_df)

    # Save functionality
    st.subheader("Salvar Alterações")
    save_option = st.radio("Salvar em:", ("S3", "Local"))

    if save_option == "S3":
        if st.button("Salvar em S3"):
            try:
                backup_key = f"backups/Controle_de_Processos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': key},
                    Key=backup_key
                )
                write_to_s3(edited_df, bucket, key)
                try:
                    read_from_s3.clear()
                except Exception:
                    pass
                st.success(f'Salvo em S3: s3://{bucket}/{key}')
                st.info(f'Backup criado em s3://{bucket}/{backup_key}')
            except Exception as e:
                st.error(f"Erro salvando em S3: {e}")

    else:
        # Opção 1: salvar no servidor (apenas se rodando local)
        save_path = st.text_input("Salvar localmente (apenas se estiver rodando local):",
                                  value="Controle_de_Processos_editado.parquet")
        if st.button("Salvar localmente (no servidor)"):
            try:
                edited_df.to_parquet(save_path, index=False, engine='pyarrow')
                st.success(f"Arquivo gravado no servidor: {save_path}")
                st.info("Se estiver no Streamlit Cloud, lembre-se: o arquivo não persistirá entre reinícios.")
            except Exception as e:
                st.error(f"Erro salvando localmente: {e}")

        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            # create Arrow table
            table = pa.Table.from_pandas(df=edited_df, preserve_index=False) # type: ignore
            buf = pa.BufferOutputStream()
            pq.write_table(table, buf)
            data_bytes = buf.getvalue().to_pybytes()

            st.download_button(
                label="Download do arquivo (.parquet)",
                data=data_bytes,
                file_name="Controle_de_Processos_editado.parquet",
                mime="application/octet-stream",
            )
        except Exception as e:
            st.error(f"Erro preparando download: {e}")

    # Footer
    st.markdown("---")
    st.markdown("""
    **Application Notes:**
    - Default loads from S3: `s3://controle-de-processos/Controle_de_Processos.parquet`
    - Upload alternative files when needed
    - All S3 operations use credentials from `~/.aws/credentials`
    """)

elif st.session_state.get('authentication_status') is False:
    st.warning("Usuário/senha inválidos.")
elif st.session_state.get('authentication_status') is None:
    st.warning("Por favor, insira usuário e senha.")