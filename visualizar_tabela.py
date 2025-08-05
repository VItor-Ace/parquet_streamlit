import string
import random
import streamlit_authenticator as stauth
import streamlit as st
import pandas as pd
import os
from datetime import datetime
import boto3
from io import BytesIO
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
                    {'Form name':'Login', 'Username':'Nome de usuário', 'Password':'Senha', 'Login':'Login', 'Captcha':'Captcha'},
                    True
                )

            except Exception as e:
                st.error(e)

        with tabs[1]:
            st.subheader("Novo registro de usuário.")

            try:
                email_of_registered_user, \
                username_of_registered_user, \
                name_of_registered_user = authenticator.register_user(fields={'Form name':'Registro', 'Email':'Email', 'Username':'Nome de usuário', 'Password':'Senha', 'Repeat password':'Repita a senha', 'Password hint':'Dica para a senha', 'Captcha':'Captcha', 'Register':'Registrar'})
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
                new_random_password = authenticator.forgot_password(fields={'Form name':'Esqueci minha senha', 'Username':'Nome de usuário', 'Captcha':'Captcha', 'Submit':'Feito!'})
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
                username_recovered = authenticator.forgot_username(fields={'Form name':'Esqueci meu usuário', 'Email':'Email', 'Captcha':'Captcha', 'Submit':'Feito!'})
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
        # Password reset button

        if st.button("Redefinir minha senha"):
            try:
                if authenticator.reset_password(st.session_state['username'], location='sidebar'):
                    with open('credentials.yaml', 'w', encoding='utf-8') as file:
                        yaml.dump(authenticator.credentials, file, allow_unicode=True)
                    st.success('Senha modificada com sucesso!')
            except Exception as e:
                st.error(f"Erro ao redefinir senha: {e}")

        # User details update button
        if st.button("Atualizar meus dados de usuário"):
            try:
                if authenticator.update_user_details(st.session_state['username'], location='sidebar'):
                    with open('credentials.yaml', 'w', encoding='utf-8') as file:
                        yaml.dump(authenticator.credentials, file, default_flow_style=False, allow_unicode=True)
                    st.success('Dados atualizados com sucesso!')
            except Exception as e:
                st.error(f"Erro ao atualizar dados: {e}")

with st.sidebar:
    if st.session_state.get('authentication_status'):
        # Password reset button
        if st.button("Redefinir minha senha"):
            try:
                username = st.session_state['username']

                if authenticator.reset_password(st.session_state['username'], location='sidebar', fields={'Form name':'Redefinir sua Senha', 'Current password':'Senha atual', 'New password':'Nova senha', 'Repeat password': 'Repita a senha', 'Reset':'Redefinir'}):
                    new_password = authenticator.credentials['usernames'][username]['password']
                    if len(new_password) < 8:
                        raise ValueError("Senha deve ter 8+ caracteres")

                    update_credentials_file()

                    if is_running_on_cloud():
                        st.warning("""
                            Senha alterada com sucesso! \n
                            **Para usuários do Streamlit Community Cloud:** \n
                            Notifique o administrador para atualizar os secrets no painel.
                            """)
                    else:
                        st.success("Senha alterada com sucesso!")

            except Exception as e:
                st.error(f"Falha na atualização: {str(e)}")
                authenticator.credentials = yaml.safe_load(open('credentials.yaml'))

        # User details update button
        if st.button("Atualizar meus dados de usuário"):
            try:
                if authenticator.update_user_details(st.session_state['username'], location='sidebar', fields={'Form name':'Atualizar detalhes de Usuário', 'Field':'Campo', 'First name':'Nome', 'Last name':'Sobrenome', 'Email':'Email', 'New value':'Novo valor', 'Update':'Atualizar'}):
                    update_credentials_file()
                    st.success('Dados atualizados com sucesso!')
            except Exception as e:
                st.error(f"Erro ao atualizar dados: {e}")
                authenticator.credentials = yaml.safe_load(open('credentials.yaml'))

if st.session_state.get('authentication_status'):
    # Limpa as abas de autenticação
    auth_container.empty()

    authenticator.logout('Sair', 'sidebar')

    # ---------------------------------- WRITING THE WEB APP INTERFACE AND COMMANDS -------------------------------------- #

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

    for i, lista in enumerate(data_file_values):
        for j, item in enumerate(lista):
            # print(f"{i=}, {j=}, {item=}, type={type(item)}")

            if pd.isna(item) or item == 'nan':
                data_file_values[i][j] = '--'
            elif isinstance(item, datetime):
                data_file_values[i][j] = item.strftime("%d/%m/%Y")  # MODIFICAÇÃO: formatar datetime direto
            elif isinstance(item, str):
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
                    try:
                        parsed_date = datetime.strptime(item, fmt).date()
                        data_file_values[i][j] = parsed_date.strftime("%d/%m/%Y")
                        break
                    except ValueError:
                        continue

    colunas = df.columns
    DataFrame_corrigido = pd.DataFrame(data_file_values, columns=colunas)

    DataFrame_corrigido.to_parquet('Controle_de_Processos.parquet', index=False)

    df = DataFrame_corrigido

    # Main editor function
    def main_editor(df_f: pd.DataFrame) -> pd.DataFrame:
        st.subheader("Edite a Tabela")
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

        def hash_row(row):
            return hash(tuple(row))

        original_keys = set(hash_row(row) for row in df_f.values)
        edited_keys = set(hash_row(row) for row in edited_df.values)

        lines_removed = original_keys - edited_keys
        added_lines = edited_keys - original_keys

        if lines_removed:
            st.warning(f'Houve(ram) {len(lines_removed)} linha(s) removida(s). Confirme a ação:')

            # Initialize session state if not exists
            if 'verification_code' not in st.session_state:
                st.session_state.verification_code = generating_random_code()
                st.session_state.verified = False

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
                        st.success("Remoção confirmada!")
                    else:
                        st.error("Código incorreto!")
                        st.session_state.verified = False

            # Only return edited DF if verified
            if not st.session_state.get('verified', False):
                return df_f  # Return original if not verified

        if added_lines:
            st.warning(f'Houve(ram) {len(lines_removed)} linha(s) adicionada(s) do arquivo original.')

        return edited_df

    # Display and edit data
    edited_df = main_editor(df)

    # Save functionality
    st.subheader("Salvar Alterações")
    save_option = st.radio("Salvar em:", ("S3", "Local"))

    if save_option == "S3":
        if st.button("Salvar como S3"):
            try:
                # Create backup first
                backup_key = f"backups/Controle_de_Processos_{datetime.now().strftime('%Y%m%d')}.parquet"
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': key},
                    Key=backup_key
                )

                # Save edited version
                write_to_s3(edited_df, bucket, key)
                st.success(f'Salvo como S3: s3://{bucket}/{key}')
                st.info(f'Backup criado em s3://{bucket}/{backup_key}')
            except s3.exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    st.error("Arquivo original não encontrado no S3")
                else:
                    st.error(f"Erro AWS: {str(e)}")
            except Exception as e:
                st.error(f"Erro inesperado: {str(e)}")
    else:
        save_path = st.text_input("Local save path:", value="Controle_de_Processos_editado.parquet")
        if st.button("Salvar localmente"):
            try:
                edited_df.to_parquet(save_path, index=False)
                st.success(f"Salvar localmente como {save_path}")
            except Exception as e:
                st.error(f"Erro salvando localmente: {str(e)}")

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











