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
    config['preauthorized']['emails']
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
                name_of_registered_user = authenticator.register_user(
                    fields={'Form name':'Registro', 'Email':'Email', 'Username':'Nome de usuário', 'Password':'Senha', 'Repeat password':'Repita a senha', 'Password hint':'Dica para a senha', 'Captcha':'Captcha', 'Register':'Registrar'}
                )
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
                    fields={'Form name':'Esqueci minha senha', 'Username':'Nome de usuário', 'Captcha':'Captcha', 'Submit':'Feito!'}
                )
                if username_of_forgotten_password:
                    st.success('Nova senha enviada com segurança!')
                    with open('credentials.yaml', 'w', encoding='utf-8') as file:
                        yaml.dump(authenticator.credentials, file, default_flow_style=False, allow_unicode=True)
                else:
                    st.error('Usuário não encontrado.')

            except Exception as e:
                st.error(e)

        with tabs[3]:
            st.subheader("Recuperar nome de usuário")
            try:
                username_recovered = authenticator.forgot_username(
                    fields={'Form name':'Esqueci meu usuário', 'Email':'Email', 'Captcha':'Captcha', 'Submit':'Feito!'}
                )
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

        # --- Reset Password ---
        if st.sidebar.button("Redefinir minha senha", key="unique_reset_pwd_btn"):
            try:
                username = st.session_state['username']
                result = authenticator.reset_password(
                    username,
                    location='sidebar',
                    fields={
                        'Form name': 'Redefinir sua Senha',
                        'Current password': 'Senha atual',
                        'New password': 'Nova senha',
                        'Repeat password': 'Repita a senha',
                        'Reset': 'Redefinir'
                    }
                )

                if result:
                    new_pwd = authenticator.credentials['usernames'][username]['password']
                    if len(new_pwd) < 8:
                        st.sidebar.error("Falha: nova senha deve ter ao menos 8 caracteres.")
                    else:
                        if st.secrets.get('streamlit_cloud', False):
                            st.sidebar.warning(
                                "Para Streamlit Cloud: contate o administrador\n"
                                "para atualizar a senha em Settings → Secrets"
                            )
                        else:
                            update_credentials_file()
                            st.sidebar.success("Senha alterada com sucesso!")
                elif not result:
                    st.sidebar.error(
                        "Não foi possível redefinir a senha.\n"
                        "Verifique a senha atual e tente novamente."
                    )
                else:  # None (usuário cancelou)
                    st.sidebar.info("Redefinição de senha cancelada.")

            except Exception as e:
                st.sidebar.error(f"Falha inesperada ao redefinir senha: {e}")

        # --- Update User Details ---
        if st.sidebar.button("Atualizar meus dados", key="unique_update_details_btn"):
            try:
                result = authenticator.update_user_details(
                    st.session_state['username'],
                    location='sidebar',
                    fields={
                        'Form name': 'Atualizar detalhes',
                        'Field': 'Campo',
                        'First name': 'Nome',
                        'Last name': 'Sobrenome',
                        'Email': 'Email',
                        'New value': 'Novo valor',
                        'Update': 'Atualizar'
                    }
                )

                if result:
                    if st.secrets.get('streamlit_cloud', False):
                        st.sidebar.warning(
                            "Para Streamlit Cloud: contate o administrador\n"
                            "para atualizar os detalhes em Settings → Secrets"
                        )
                    else:
                        update_credentials_file()
                        st.sidebar.success("Dados atualizados com sucesso!")
                else:
                    st.sidebar.error("Não foi possível atualizar os dados. Reveja as informações e tente novamente.")

            except Exception as e:
                st.sidebar.error(f"Erro inesperado ao atualizar dados: {e}")

        # --- Logout ---
        authenticator.logout('Sair', 'sidebar')

if st.session_state.get('authentication_status'):
    # limpa as abas de autenticação
    auth_container.empty()

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
    novos_nomes = []
    contador_pontos = 0
    for nome in df.columns:
        if nome == '.':
            novos_nomes.append(str(contador_pontos))
            contador_pontos += 1
        else:
            novos_nomes.append(nome)
    df.columns = novos_nomes

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
        try:
            edited_df = st.data_editor(
                df_f,
                use_container_width=True,
                num_rows="dynamic",
                key="data_editor"
            )
        except AttributeError:
            edited_df = st.experimental_data_editor(
                df_f,
                use_container_width=True,
                num_rows="dynamic",
                key="data_editor"
            )

        original_keys = {hash(tuple(row)) for row in df_f.values}
        edited_keys = {hash(tuple(row)) for row in edited_df.values}

        lines_removed = original_keys - edited_keys
        added_lines = edited_keys - original_keys

        if lines_removed:
            st.warning(f'Houve(ram) {len(lines_removed)} linha(s) removida(s). Confirme a ação:')
            if 'verification_code' not in st.session_state:
                st.session_state.verification_code = generating_random_code()
                st.session_state.verified = False

            col1, col2 = st.columns([3, 1])
            with col1:
                user_input = st.text_input(
                    f"Digite '{st.session_state.verification_code}' para confirmar",
                    key="verification_input"
                )
            with col2:
                if st.button("Confirmar", key="verify_button"):
                    if user_input == str(st.session_state.verification_code):
                        st.session_state.verified = True
                        st.success("Remoção confirmada!")
                    else:
                        st.error("Código incorreto!")
                        st.session_state.verified = False

            if not st.session_state.get('verified', False):
                return df_f

        if added_lines:
            st.info(f'Foram adicionadas {len(added_lines)} linha(s) ao original.')

        return edited_df

    edited_df = main_editor(df)
    edited_df = processar_datas(edited_df)

    # Save functionality
    st.subheader("Salvar Alterações")
    save_option = st.radio("Salvar em:", ("S3", "Local"))

    if save_option == "S3":
        if st.button("Salvar em S3"):
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
                # Clear cache so next load fetches updated file
                read_from_s3.clear()

                st.success(f'Salvo em S3: s3://{bucket}/{key}')
                st.info(f'Backup criado em s3://{bucket}/{backup_key}')
            except Exception as e:
                st.error(f"Erro salvando em S3: {str(e)}")
    else:
        save_path = st.text_input("Local save path:", value="Controle_de_Processos_editado.parquet")
        if st.button("Salvar localmente"):
            try:
                edited_df.to_parquet(save_path, index=False)
                st.success(f"Salvo localmente como {save_path}")
            except Exception as e:
                st.error(f"Erro salvando localmente: {str(e)}")

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




















