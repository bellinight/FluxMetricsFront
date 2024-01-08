import streamlit as st
import pandas as pd
import time
from sqlalchemy import create_engine as ce
from streamlit_autorefresh import st_autorefresh
from datetime import datetime

############################ -----Configurações de Layout da HOME -----######################
st.set_page_config(page_title="Front FLUX", page_icon=":chart",
                   layout="wide", initial_sidebar_state="auto", menu_items=None)


# Hack de Background retirado do link: https://medium.com/@hriskikesh.yadav332/setting-cool-background-image-in-streamlit-app-cab8c8ee2a8f

page_element = """
<style>
[data-testid="stAppViewContainer"]{
  background-image: url("https://connectaiptv.com/fluxmanager/fundo-mkt-front_flux.jpg");
  background-size: cover;
}
[data-testid="stHeader"]{
  background-color: rgba(0,0,0,0);
}
[data-testid="stToolbar"]{
  right: 2rem;
  background-color: transparent;
  background-size: cover;
}
</style>
"""
st.markdown(page_element, unsafe_allow_html=True)
########################## Funções Distintas ###############################################


def recarregar_home():
    time.sleep(9000)
    st.cache_resource.clear()


def anin_atualizando():
    with st.status("Atualizando...", expanded=True) as status:
        time.sleep(10)
        st.write(
            "Lendo dados do PDV...")
        load_data_pdv()
        st.write(
            "Instalando PDV...")
        time.sleep(10)

        status.update(
            label="Instalação Finalizada!", state="complete", expanded=False)
        st.success('Pronto!')


def clear_resource():
    st.cache_data.clear()


############################# -----Conexão para extração de dados-----################################
# Conectar ao banco de dados
username_bd = st.secrets['username']
pass_bd = st.secrets["password"]
name_bd = st.secrets["database"]
host_bd = st.secrets["host"]
mydb = ce(
    f'mysql+mysqlconnector://{username_bd}:{pass_bd}@{host_bd}/{name_bd}')

##################### Query Principal Central ##############################
conc_query = "SELECT * FROM recebe_dados_conc"
pdv_query = (f"SELECT * FROM recebe_dados_pdv")
############################## Sidebar de TItulo e LOGO da aplicação ######################################
with st.sidebar:

    col1, col2, col3 = st.columns([1, 6, 1])
    with col1:
        st.write()
    with col2:
        # st.header('FRONT FLUX')
        st.markdown(
            "<h1 style='text-align: center; margin: 0 0 0 0;'>FRONT FLUX</h1>", unsafe_allow_html=True)
        st.image("top-logo_P.png", width=None)
        # st.markdown( "<h5 style='text-align: center; margin: 0 0 0 0;'>Gerencie seu Frente de Loja</h5>", unsafe_allow_html=True)
    with col3:
        st.write()

    with st.expander('Atualizar DashBoard', expanded=False):
        st.subheader("Carregar dados",
                     help="Atualiza o Front Flux e apresenta os últimos dados enviados ao servidor")
        st.button("Buscar dados", on_click=clear_resource, help="Click no botão para esvaziar o cache e atualizar os dados na dashboard.",
                  key='btndadosupdate')

    ############################### Funções para Leitura e armazenamento de dados do Front Flux ###################################

        @st.cache_data(ttl=600)  # 👈 Add the caching decorator
        def load_data_conc():

            result_conc = pd.read_sql(conc_query, mydb)

            return result_conc

        @st.cache_data(ttl=600)  # 👈 Add the caching decorator
        def load_data_pdv():

            result_pdv = pd.read_sql(pdv_query, mydb)

            return result_pdv

        with st.empty():
            result_conc = load_data_conc()
            result_pdv = load_data_pdv()
            st.write()


############################ Monta Dados Solicitados na Query Principal ########################################
# result_conc = conn.query('SELECT * FROM recebe_dados_conc;', ttl=600)
# result_conc = pd.read_sql(conc_query, mydb)
result_conc = result_conc.sort_values(by='controlID', ascending=False)
result_conc = result_conc.drop_duplicates(subset=['rede_lojas', 'cli', 'loj', 'razsoc'])[['controlID', 'rede_lojas', 'cli', 'loj', 'razsoc', 'dte_atu', 'ver_flux', 'ver_jv', 'ver_pgs', 'con_ver',
                                                                                          'hd_free', 'tam_hd', 'perc_hd', 'bkp_kb_con', 'sem_id_carga', 'sem_data_carga', 'ser_sgbd', 'ser_carg_on', 'ser_tk_app', 'ser_dbridge', 'pv_con', 'uv_con', 'integracao_notas_dr', 'integracao_notas_mc']]
result_conc_ori = result_conc[['controlID', 'rede_lojas', 'cli', 'loj', 'razsoc', 'dte_atu', 'ver_flux', 'ver_jv', 'ver_pgs', 'con_ver', 'hd_free', 'tam_hd',
                               'perc_hd', 'bkp_kb_con', 'sem_id_carga', 'sem_data_carga', 'ser_sgbd', 'ser_carg_on', 'ser_tk_app', 'ser_dbridge', 'pv_con', 'uv_con', 'integracao_notas_dr', 'integracao_notas_mc']]
result_conc_filter = pd.unique(result_conc["cli"])

################################ Variáveis da Central #############################################
conc_rede_lojas = result_conc_ori['rede_lojas'].unique()
conc_cliente = result_conc['cli']
conc_loja = result_conc_ori['loj'].unique()
conc_razsoc = result_conc['razsoc']
controlid_dados_completo = result_conc_ori.head(1)
controlid_razao = controlid_dados_completo['razsoc'].to_list()[0]
controlid_rede = controlid_dados_completo['rede_lojas'].to_list()[0]
################################## Variável para busca de ultima versão da Central ######################
mlogic_ver_hom = result_conc_ori.sort_values(by='con_ver', ascending=False)
mlogic_ver_hom = mlogic_ver_hom['con_ver'].to_list()[0]
################################## Variável para busca de data e hora local ######################
data_hora_atual_now = datetime.now()
data_hora_atual = datetime.now().date()
data_hora_atual = data_hora_atual.strftime('%d/%m/%y')
################################## Variáveis para contagem de clientes e lojas ######################
sum_lojas = len(result_conc.loj)
conta_clientes = result_conc['rede_lojas'].unique()
sum_clientes = len(conta_clientes)
########################### Variáveis de Notificação de Erros ###########################################
notifica_atualizaveis_conc = result_conc_ori[result_conc_ori['con_ver']
                                             < mlogic_ver_hom]
notifica_atualizaveis_pend = notifica_atualizaveis_conc[[
    'rede_lojas', 'razsoc']]
numero_lojas_atualizaveis = len(notifica_atualizaveis_pend)

notifica_atualizadas_conc = result_conc_ori[result_conc_ori['con_ver']
                                            == mlogic_ver_hom]
notifica_atualizadas_cent = notifica_atualizadas_conc[[
    'rede_lojas', 'razsoc']]
numero_lojas_atualizadas = len(notifica_atualizadas_cent)

########################### Variáveis de Notificação de Erros Ponto de Venda ###########################################


############################# ----- Conexão PDV´s ----- #############################
result_pdv_ori = result_pdv[result_pdv['ult_ca'] != '102324']
result_pdv_ori['dados_nfce_pdv_data_fech'] = pd.to_datetime(
    result_pdv_ori['dados_nfce_pdv_data_fech'])
result_pdv_ori = result_pdv_ori.sort_values(
    by='dados_nfce_pdv_data_fech', ascending=False)

result_pdv = result_pdv.sort_values(by='ID', ascending=False)
result_pdv = result_pdv.drop_duplicates(
    subset=['cli', 'loj', 'pdv'])[['rede_lojas', 'cli', 'loj', 'pdv', 'pdv_at', 'pdv_ve', 'ult_ca', 'pdv_at_id', 'pdv_rej', 'dados_nfce_pdv_numero', 'dados_nfce_pdv_situacao', 'dados_nfce_pdv_valor', 'dados_nfce_pdv_usuario', 'dados_nfce_pdv_enviado', 'dados_nfce_pdv_data_fech', 'dados_nfce_pdv_scanntech', 'dados_nfce_pdv_email', 'dados_nfce_pdv_url_code', 'dados_nfce_pdv_nfemissao']]
######################### Variáveis de Filtro para PDV #########################
result_pdv_filter = pd.unique(result_pdv['loj'])
pdvs = result_pdv[['rede_lojas', 'pdv', 'loj', 'cli']]
sum_pdvs = len(pdvs)

###########################  CAMPO NA SIDEBAR PARA SELEÇÃO DA REDE ##############################

with st.sidebar:
    # selecao_rede_me = 'nomedarede'
    selecao_rede_me = st.selectbox(
        'Selecione o Cliente na Lista', conc_rede_lojas, index=None, placeholder="Digite ou Click", help="Selecione a rede para apresentação das lojas.")

    conc_dados_completos = result_conc_ori[result_conc_ori['rede_lojas']
                                           == f"{selecao_rede_me}"]
    st.caption(f"Last Data: \n {controlid_rede} - {controlid_razao}")
#######################################################
st.sidebar.subheader('Opções de Visualização')
########################### Dados Gerenciais ###################################
if st.sidebar.toggle('Dados Gerenciais', value=True):
    with st.container():
        col01, col02, col03, col04, col05, col06 = st.columns(
            [1, 1, 1, 1, 2, 2])
        # st.write(grupo4)
        with col01:
            st.metric("Clientes", f"{sum_clientes}", f'Meta 50')
            st.write("")
        with col02:
            st.metric("Lojas Atendidas", f"{sum_lojas}", f"Meta 100")
            st.write("")
        with col03:
            st.metric(f"Lojas Atualizadas - {mlogic_ver_hom}", f"{numero_lojas_atualizadas}",
                      f"Defict {numero_lojas_atualizaveis}")
            st.write("")
        with col04:
            st.metric("PDV´s Gerenciados", f"{sum_pdvs}", f"Meta 1000")
            st.write("")
        with col05:
            with st.expander(f"Centrais Atualizadas - {numero_lojas_atualizadas}"):
                st.write()
                for i6, grupo6 in notifica_atualizadas_cent.groupby('razsoc'):
                    atualizadas_cent_rede = i6
                    atualizadas_cent_rede_nome = grupo6['rede_lojas'].to_list()[
                        0]

                    if not notifica_atualizadas_cent.empty:
                        st.success(
                            f"||{atualizadas_cent_rede_nome}|| - {atualizadas_cent_rede}")
                    else:
                        st.write("SEM ERROS PARA APRESENTAR")

                # st.write(grupo6)
            total_alertas_scantec = result_pdv[result_pdv['dados_nfce_pdv_scanntech'] == '0']
            total_alertas_scantec = len(total_alertas_scantec['rede_lojas'])
            with st.expander(f"Alertas Scanntech - :name_badge: {total_alertas_scantec} PDV´s"):
                pdv_scantec_offline = result_pdv[result_pdv['dados_nfce_pdv_scanntech'] == '0']
                pdv_scantec_offline = pdv_scantec_offline[[
                    'rede_lojas', 'pdv', 'loj']]

                for i7, grupo7 in pdv_scantec_offline.groupby('pdv') and pdv_scantec_offline.groupby('rede_lojas'):
                    scantecerros_cli = i7
                # grupo7
                grupo7_format_pdv = pd.Series(grupo7['pdv']).to_list()[
                    :]
                grupo7_format_strings_pdv = [str(valor)
                                             for valor in grupo7_format_pdv]
                grupo7_format_loj = pd.Series(grupo7['loj']).to_list()[
                    :]
                grupo7_format_strings_loj = [str(valor)
                                             for valor in grupo7_format_loj]

                if not pdv_scantec_offline.empty:

                    if st.toggle(f'REDE: {scantecerros_cli}', key=scantecerros_cli):
                        for string_valor_loj in grupo7_format_strings_loj:
                            serie_pdv_alert_scntc_loj = string_valor_loj
                        for string_valor_pdv in grupo7_format_strings_pdv:
                            serie_pdv_alert_scntc_pdv = string_valor_pdv
                            st.warning(
                                F"Loja:{string_valor_loj} - {string_valor_pdv}")
                    else:
                        st.write()
                else:
                    st.write("SEM ERROS PARA APRESENTAR")
        with col06:
            with st.expander(f"Centrais Desatualizadas - {numero_lojas_atualizaveis}"):
                st.write()
                # notifica_erros_carga_rede_bridge = notifica_erros_bridge['rede_lojas'].to_list()[0]
                for i5, grupo5 in notifica_atualizaveis_pend.groupby('razsoc'):
                    atualizaveis_cent_rede = i5
                    atualizaveis_cent_rede_nome = grupo5['rede_lojas'].to_list()[
                        0]

                    if not notifica_atualizaveis_pend.empty:
                        st.error(
                            f"||{atualizaveis_cent_rede_nome}|| - {atualizaveis_cent_rede}")
                    else:
                        st.write("SEM ERROS PARA APRESENTAR")

                # st.write(i5)
                    total_alertas_nfce_pend = result_conc_ori[result_conc_ori['integracao_notas_dr'] > '0']
                total_alertas_nfce_pend = len(
                    total_alertas_nfce_pend['rede_lojas'])

            with st.expander(f"NFCe´s Pendentes - :name_badge: {total_alertas_nfce_pend} lojas"):
                nfce_pend_int = result_conc_ori[result_conc_ori['integracao_notas_dr'] > '0']
                nfce_pend_int = nfce_pend_int[[
                    'rede_lojas', 'loj', 'integracao_notas_dr']]

                for i8, grupo8 in nfce_pend_int.groupby('rede_lojas'):
                    dados_nfce_pend_int = i8
                    if not nfce_pend_int.empty:
                        st.error(
                            f"{dados_nfce_pend_int}", icon="🚨")
                    else:
                        st.write("SEM ERROS PARA APRESENTAR")
if st.sidebar.toggle("Notificações e Erros", value=True):
    with st.container():
        col1, col2, col3, col4 = st.columns(4)
        ########################################################################
        with col1:
            st.caption("ERROS NO SGBD")
            notifica_erros_sgbd = result_conc_ori[result_conc_ori['ser_sgbd'] == 'Stopped']
            notifica_erros_sgbd = notifica_erros_sgbd[[
                'rede_lojas', 'razsoc', 'ser_sgbd']]
            # notifica_erros_carga_rede_sgbd = notifica_erros_sgbd['rede_lojas'].to_list()[0]
            for i1, grupo1 in notifica_erros_sgbd.groupby('razsoc'):
                sgbd_erros_rede = i1
                if not notifica_erros_sgbd.empty:
                    st.error(
                        f'{sgbd_erros_rede}', icon="🚨")
                if notifica_erros_sgbd.empty:
                    st.write("SEM ERROS PARA APRESENTAR")
            ########################################################################
        with col2:
            st.caption("NFCE - TOOLKIT")
            notifica_erros_tk = result_conc_ori[result_conc_ori['ser_tk_app'] == 'Stopped']
            notifica_erros_tk = notifica_erros_tk[[
                'rede_lojas', 'razsoc', 'ser_tk_app']]
            # notifica_erros_carga_rede_tk = notifica_erros_tk['rede_lojas'].to_list()[0]
            for i2, grupo2 in notifica_erros_tk.groupby('razsoc'):
                tk_erros_rede = i2
                if not notifica_erros_tk.empty:
                    st.error(
                        f"{tk_erros_rede}", icon="🚨")
                    st.toast(f'TK OFFLINE - Verifique a lista')
                if notifica_erros_tk.empty:
                    st.write("SEM ERROS PARA APRESENTAR")
            ########################################################################
        with col3:
            st.caption("CARGA ONLINE")
            notifica_erros_carga = result_conc_ori[result_conc_ori['ser_carg_on'] == 'Stopped']
            notifica_erros_carga = notifica_erros_carga[[
                'rede_lojas', 'razsoc', 'ser_carg_on']]
            # notifica_erros_carga_rede_carga = notifica_erros_carga['rede_lojas'].to_list()[0]
            for i3, grupo3 in notifica_erros_carga.groupby('razsoc'):
                carga_erros_rede = i3
                if not notifica_erros_carga.empty:
                    st.error(
                        f"{carga_erros_rede}", icon="🚨")
                else:
                    st.write("SEM ERROS PARA APRESENTAR")
            ########################################################################
        with col4:
            st.caption("DATABRIDGE")
            notifica_erros_bridge = result_conc_ori[result_conc_ori['ser_dbridge'] == 'Stopped']
            notifica_erros_bridge = notifica_erros_bridge[[
                'rede_lojas', 'razsoc', 'ser_dbridge']]
            # notifica_erros_carga_rede_bridge = notifica_erros_bridge['rede_lojas'].to_list()[0]
            for i4, grupo4 in notifica_erros_bridge.groupby('razsoc'):
                bridge_erros_rede = i4

                if not notifica_erros_bridge.empty:
                    st.error(
                        f"{bridge_erros_rede}", icon="🚨")
                else:
                    st.write("SEM ERROS PARA APRESENTAR")


# Layout para apresentação de dados da Central e PDV´s
with st. container():
    for cli, grupoconc in conc_dados_completos.groupby('cli'):

        # st.table(grupoconc)

        nome_loja_checkbox = grupoconc['razsoc'].to_list()[0]

        checkbox_selecao_loja = st.checkbox(
            f':convenience_store: {nome_loja_checkbox}', value=True, key=conc_cliente+cli, help=f" :convenience_store: Nome da Loja - :hammer_and_wrench: Data e hora dos dados armazenados.")
        if checkbox_selecao_loja is True:
            if selecao_rede_me is not None:
                lista_dados_conc = conc_dados_completos[conc_dados_completos['cli']
                                                        == cli]
                lista_dados_conc['sem_data_carga'] = pd.to_datetime(
                    lista_dados_conc['sem_data_carga'])
               # st.table(lista_dados_conc)

                ############ Monta Apresentação de dados do Concentrador ##########################
                lojas_rede = lista_dados_conc['rede_lojas'].to_list()[0]
                id_cli = lista_dados_conc['cli'].to_list()[0]
                id_loja = lista_dados_conc['loj'].to_list()[0]
                raz_loja = lista_dados_conc['razsoc'].to_list()[0]
                atu_conc = lista_dados_conc['dte_atu'].to_list()[0]
                flux_ve = lista_dados_conc['ver_flux'].to_list()[0]
                java_ve = lista_dados_conc['ver_jv'].to_list()[0]
                pgs_ve = lista_dados_conc['ver_pgs'].to_list()[0]
                con_ve = lista_dados_conc['con_ver'].to_list()[0]
                hd_livre_gb = lista_dados_conc['hd_free'].to_list()[0]
                hd_tam_gb = lista_dados_conc['tam_hd'].to_list()[0]
                hd_livre = lista_dados_conc['perc_hd'].to_list()[0]
                tam_bkp_conc = lista_dados_conc['bkp_kb_con'].to_list()[0]
                tam_bkp_conc = round(float(tam_bkp_conc) / (1024 ** 2), 2)
                id_carga_conc = lista_dados_conc['sem_id_carga'].to_list()[0]
                data_carga_conc = lista_dados_conc['sem_data_carga'].dt.strftime(
                    '%d/%m/%y').to_list()[0]
                ser_sgbd = lista_dados_conc['ser_sgbd'].to_list()[0]
                ser_carg_on = lista_dados_conc['ser_carg_on'].to_list()[0]
                ser_tk_app = lista_dados_conc['ser_tk_app'].to_list()[0]
                ser_dbridge = lista_dados_conc['ser_dbridge'].to_list()[0]
                implantacao_data = lista_dados_conc['pv_con'].to_list()[0]
                ultima_venda_data = lista_dados_conc['uv_con'].to_list()[0]
                integracao_notas_dr = lista_dados_conc['integracao_notas_dr'].to_list()[
                    0]
                integracao_notas_mc = lista_dados_conc['integracao_notas_mc'].to_list()[
                    0]
                #####  CALCULA TOTAL DE PDVS NA LOJA #########
                total_pdvs_loja = result_pdv[result_pdv['cli'] == id_cli]
                total_pdvs_loja = total_pdvs_loja['pdv']
                total_pdvs_loja = len(total_pdvs_loja)
                # Pega data e hora da NFCe e coloca como ultimo recebimento de dados

                pdv_last_nfce = result_pdv_ori[result_pdv_ori['cli'] == id_cli]
                pdv_last_nfce = pdv_last_nfce['dados_nfce_pdv_data_fech'].dt.strftime(
                    '%d/%m/%y %H:%M:%S').to_list()[
                    0]

                ####### LAYOUT DE APRESENTAÇÃO DE DADOS DA CENTRAL ###########
                col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14 = st.columns(
                    14)

                with col1:
                    st.caption(
                        f"PDV´s ", help="Soma de todos os PDV´s existentes na loja")

                    st.info(f"Total: {total_pdvs_loja}")

                with col2:
                    st.caption("Central", help="Versão do Concentrador.")
                    if (f"{con_ve}") == " ":
                        st.error(f"ERROR")
                    elif (f"{con_ve}") < mlogic_ver_hom:
                        st.warning(f' {con_ve}')
                    else:
                        st.success(f"{con_ve}")

                with col3:
                    st.caption(
                        "JAVA", help="Versão do Java instalado no Concentrador.")
                    st.info(f"{java_ve}")

                with col4:
                    st.caption(
                        "HDSCAN", help="Percentual de uso do HD")
                    if hd_livre <= (f'60'):
                        st.success(f"{hd_livre} %")
                    elif hd_livre <= (f'80'):
                        st.warning(f"{hd_livre} %")
                    else:
                        st.error(f"{hd_livre} %")

                with col5:
                    st.caption(
                        "Backup", help="Tamanho |EM GB| do Backup")
                    st.info(f"{tam_bkp_conc}")

                with col6:
                    st.caption(
                        "Ult. Carga", help="Data do envio da ultima carga.")
                    st.info(f"{data_carga_conc}")

                with col7:
                    st.caption(
                        "ID da carga", help="Identificação de controle da carga enviada.")
                    st.info(f"{id_carga_conc}")

                with col8:
                    st.caption(
                        "SGBD", help="Status do serviço do SGBD do Concentrador. HOMOLOGADO: Postgresql")
                    if 'Stopped' in lista_dados_conc['ser_sgbd'].to_list():
                        st.error("SGBD :rotating_light:")
                    elif 'Running' in lista_dados_conc['ser_sgbd'].to_list():
                        st.success(f"{pgs_ve}")
                    else:
                        st.warning(f"{pgs_ve}")
                with col9:
                    st.caption(
                        "Mlogic", help="Status do serviço de carga online")
                    if 'Stopped' in lista_dados_conc['ser_carg_on'].to_list():
                        st.error("Mlogic :rotating_light:")
                    elif 'Running' in lista_dados_conc['ser_carg_on'].to_list():
                        st.success(f"OK")
                    else:
                        st.warning("Verificar")
                with col10:
                    st.caption(
                        "NFCe", help="Status do serviço de autorização de notas fiscais.")
                    if 'Stopped' in lista_dados_conc['ser_tk_app'].to_list():
                        st.error("TK :rotating_light:")
                    elif 'Running' in lista_dados_conc['ser_tk_app'].to_list():
                        st.success(f"OK")
                    else:
                        st.warning("Verificar")
                with col11:
                    st.caption(
                        "Bridge", help="Status do serviço de API  <=> CENTRAL")
                    if 'Stopped' in lista_dados_conc['ser_dbridge'].to_list():
                        st.error("Bridge :rotating_light:")
                    elif 'Running' in lista_dados_conc['ser_dbridge'].to_list():
                        st.success(f"OK")
                    else:
                        st.warning("Verificar")
                with col12:
                    st.caption(
                        "Integradas", help="Numero de notas integradas, nos últimos 31 dias.")
                    st.info(f"{integracao_notas_mc}")
                with col13:
                    st.caption(
                        "Pendentes", help="Numero de notas pendentes de integração, nos últimos 31 dias.")
                    if integracao_notas_dr == "0":
                        st.success(f"{integracao_notas_dr}")
                    else:
                        st.error(f"{integracao_notas_dr}")
                with col14:
                    st.caption(
                        "Detalhes", help="Click para ativar a opção desejada. (:shopping_trolley:) Lista de PDV´s. (:moneybag:) Dados Fechamento do dia anterior")
                    mostrar_pdvs = st.toggle(
                        f':shopping_trolley:', key=id_cli+id_loja+'1')
                    mostrar_dados_fin = st.toggle(
                        f':moneybag:', key=id_cli+id_loja+'2')

                if mostrar_dados_fin:
                    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
                    with col1:
                        st.caption('Total Redução :moneybag:')
                        st.info("R$ 27.585,23")
                    with col2:
                        st.caption('Total Itens :moneybag:')
                        st.info("R$ 27.585,23")
                    with col3:
                        st.caption('Total Nfc´s :moneybag:')
                        st.info("R$ 27.585,23")
                    with col4:
                        st.write()
                    with col5:
                        st.caption('Total Sangrias :moneybag:')
                        st.info("R$ 2.500,00")

                    with col6:
                        st.caption('Lucro % medio :moneybag:')
                        st.info("32%")
                    with col7:
                        st.caption('CAC :moneybag:')
                        st.info("R$ 85,23")

                st.caption(f":hammer_and_wrench: {pdv_last_nfce}")
                st.divider()

                with st.empty():
                    if mostrar_pdvs:
                        ########################################################################
                        pdv_dados_completos = result_pdv[result_pdv['cli'] == cli]

                        with st.container():
                            # st.divider()
                            st.subheader(
                                f':card_file_box: PDV´s Analisados: {total_pdvs_loja}', help="Nome da loja acessada para verificação.")
                            st.caption(
                                "Verifique abaixo os detalhes do frente de loja")

                            # isall = st.sidebar.checkbox(label="Selecionar Todos", value=True, key=id_cli+hd_livre)
                            for (pdv, cli), grupopdv in pdv_dados_completos.groupby(['pdv', 'cli']):

                                # st.table(grupopdv)

                                # info_pdv_dados = st.sidebar.checkbox(f"{pdv}", value=isall)
                                # if info_pdv_dados:
                                lista_dados_pdv = pdv_dados_completos[pdv_dados_completos['pdv'] == pdv]

                                lista_dados_pdv['ult_ca'] = pd.to_datetime(
                                    lista_dados_pdv['ult_ca'])
                                lista_dados_pdv['dados_nfce_pdv_data_fech'] = pd.to_datetime(
                                    lista_dados_pdv['dados_nfce_pdv_data_fech'])
                                ############ Monta Apresentação de dados do PDV ##########################
                                lojas_rede = lista_dados_pdv['rede_lojas'].to_list()[
                                    0]
                                id_cli_pdv = lista_dados_pdv['cli'].to_list()[
                                    0]
                                id_loj_pdv = lista_dados_pdv['loj'].to_list()[
                                    0]
                                id_pdv = lista_dados_pdv['pdv'].to_list()[
                                    0]
                                pdv_atu = lista_dados_pdv['pdv_at'].to_list()[
                                    0]
                                ve_pdv = lista_dados_pdv['pdv_ve'].to_list()[
                                    0]
                                pdv_carg = lista_dados_pdv['ult_ca'].dt.strftime(
                                    '%d/%m/%y').to_list()[0]

                                id_pdv_carg = lista_dados_pdv['pdv_at_id'].to_list()[
                                    0]

                                pdv_rej = lista_dados_pdv['pdv_rej'].to_list()[
                                    0]
                                dados_nfce_pdv_numero = lista_dados_pdv['dados_nfce_pdv_numero'].to_list()[
                                    0]
                                dados_nfce_pdv_situacao = lista_dados_pdv['dados_nfce_pdv_situacao'].to_list()[
                                    0]

                                dados_nfce_pdv_valor = lista_dados_pdv['dados_nfce_pdv_valor'].to_list()[
                                    0]

                                dados_nfce_pdv_usuario = lista_dados_pdv['dados_nfce_pdv_usuario'].to_list()[
                                    0]
                                dados_nfce_pdv_enviado = lista_dados_pdv['dados_nfce_pdv_enviado'].to_list()[
                                    0]
                                dados_nfce_pdv_data_fech = lista_dados_pdv['dados_nfce_pdv_data_fech'].dt.strftime(
                                    '%d/%m/%y').to_list()[0]
                                dados_nfce_pdv_data_hora = lista_dados_pdv['dados_nfce_pdv_data_fech'].dt.strftime(
                                    '%d/%m - %H:%M:%S').to_list()[0]
                                dados_nfce_pdv_scanntech = lista_dados_pdv['dados_nfce_pdv_scanntech'].to_list()[
                                    0]
                                dados_nfce_pdv_email = lista_dados_pdv['dados_nfce_pdv_email'].to_list()[
                                    0]
                                dados_nfce_pdv_url_code = lista_dados_pdv['dados_nfce_pdv_url_code'].to_list()[
                                    0]
                                dados_nfce_pdv_nfemissao = lista_dados_pdv['dados_nfce_pdv_nfemissao'].to_list()[
                                    0]

                                ############ Monta layout para apresentação dos dados e erros ##########################
                                # st.table(lista_dados_pdv)
                                col01, col02, col03, col04, col05, col06, col07, col08, col09, col10 = st.columns(
                                    10)

                                with col01:
                                    st.caption(
                                        ' :shopping_trolley: PDV', help="Serie do PDV")
                                    st.info(f"{id_pdv}")
                                with col02:
                                    st.caption(
                                        'Scanntech', help="NFce emitida com API Scanntech ativada.")
                                    if (f"{dados_nfce_pdv_scanntech}") != '1':
                                        st.error(f"Desativada")
                                    elif (f"{ve_pdv}") == '102324':
                                        st.warning(f"OFF")
                                    else:
                                        st.success(f"Enviada")

                                with col03:
                                    st.caption(
                                        'Ult. Carga', help="Data de registro da ultima carga enviada para os PDV´s")
                                    if (f"{pdv_carg}") == '23/10/24':
                                        st.warning(f"OFF")
                                    elif (f"{pdv_carg}") < data_hora_atual:
                                        st.error(f"{pdv_carg}")
                                    else:
                                        st.success(f"{pdv_carg}")
                                with col04:
                                    st.caption(
                                        'ID da Carga', help="ID da carga gerada e enviada aos PDV´s")
                                    if (f"{id_pdv_carg}") < id_carga_conc:
                                        st.error(f'VERIFIQUE')
                                        st.toast(
                                            f'{id_pdv},Trabalhando com Preços Antigos!', icon='😡')
                                    elif (f"{ve_pdv}") == '102324':
                                        st.warning(f"OFF")
                                    else:
                                        st.success(f"{id_pdv_carg}")

                                with col05:
                                    st.caption(
                                        'VERSÃO', help="Versão do PDV")
                                    if (f"{ve_pdv}") == " ":
                                        st.error(f"ERROR")
                                    elif (f"{ve_pdv}") == '102324':
                                        st.warning(f"OFF")
                                    elif (f"{ve_pdv}") < con_ve:
                                        st.warning(f' {ve_pdv}')
                                    else:
                                        st.success(f"{ve_pdv}")

                                with col06:
                                    st.caption(
                                        'Ult. Venda', help="Data da ultima emissão de NFCe")
                                    if (f"{dados_nfce_pdv_data_fech}") < data_carga_conc:
                                        st.error(
                                            f"OCIOSO")
                                    elif (f"{dados_nfce_pdv_data_fech}") < data_hora_atual:
                                        st.error(
                                            f"{dados_nfce_pdv_data_fech}")
                                    elif (f"{dados_nfce_pdv_data_fech}") < (f"{pdv_carg}"):
                                        st.error(f"OFFLINE")
                                        st.toast(
                                            f'{id_pdv}, OFFLINE!', icon='😡')
                                    elif (f"{ve_pdv}") == '102324':
                                        st.warning(f"OFF")
                                    else:
                                        st.success(
                                            f"{dados_nfce_pdv_data_hora}")

                                with col07:
                                    st.caption(
                                        'Ult. NFCe', help="Numero da ultima NFCe emitida no PDV")
                                    if (f"{ve_pdv}") == '102324':
                                        st.warning(f"OFF")
                                    else:
                                        st.info(
                                            f"{dados_nfce_pdv_numero}")
                                with col08:
                                    st.caption(
                                        'SEFAZ', help="Resposta da SEFAZ para a autorização da NFCe. Se a rejeição apresentar numero diferente de 100, acesse o TOOLKIT para verificação.")
                                    if (f"{pdv_rej}") == '100':
                                        st.success(f"{pdv_rej}")
                                    elif (f"{pdv_rej}") == '539':
                                        st.error(f"ERROR {pdv_rej}")
                                    elif (f"{pdv_rej}") != '100':
                                        st.warning(f"{pdv_rej}")
                                    else:
                                        st.error(f"Sem Emissão")
                                with col09:
                                    st.caption(
                                        "Ult. Transação", help="Click e acesse os dados da ultima transação no PDV")
                                    detalhes_do_pdv = st.button(
                                        f'Verificar', key=id_cli+id_pdv)
                                with col10:
                                    st.write("")
                                    if (f"{ve_pdv}") < con_ve:
                                        st.write("Versão 📛")
                                        if st.button(f"Atualizar", key=id_cli+id_loja+id_pdv, help="Click no botão para efetuar a atualização do PDV"):
                                            anin_atualizando()
                                    else:
                                        st.write("Versão ✅")

                                with st.container():
                                    # st.divider()
                                    if detalhes_do_pdv:
                                        col1, col2, col3, col4, col5, col6, col7 = st.columns(
                                            [2, 1, 1, 1, 1, 1, 1])
                                        st.caption(
                                            "Detalhes da ultima transação")
                                        with col1:
                                            st.caption(
                                                'Usuário', help="Usuário Logado na hora da Transação")
                                            st.info(
                                                f'{dados_nfce_pdv_usuario}')
                                        with col2:
                                            st.caption(
                                                'Nº NFCe', help="Numero da NFCE na hora da Transação")
                                            st.info(
                                                f'{dados_nfce_pdv_numero}')
                                        with col3:
                                            st.caption(
                                                'VALOR', help="Valor total da NFCe no momento da Transação")
                                            st.info(
                                                f"{dados_nfce_pdv_valor}")
                                        with col4:
                                            st.caption(
                                                'ENVIADA', help='Integrada com o Concentrador')
                                            if (f"{dados_nfce_pdv_enviado}") != "1":
                                                st.error(f'Não')
                                            else:
                                                st.success(f"Sim")
                                        with col5:
                                            st.caption(f"Hora da Transação")
                                            st.info(
                                                f"{dados_nfce_pdv_data_hora}")
                                        with col6:
                                            st.caption(
                                                'EMAIL', help="Mostra se a NFCe foi enviada para email do cliente")
                                            if (f"{dados_nfce_pdv_email}") != '1':
                                                st.error(f'Não')
                                            else:
                                                st.success(f"Sim")
                                        with col7:
                                            st.caption(
                                                'Nota Online', help="Verificar nota diretamente no site da SEFAZ")
                                            st.link_button(
                                                "Visualizar", f"{dados_nfce_pdv_url_code}")
                                        st.divider()
                                    else:
                                        st.write()
                            st.sidebar.divider()
                            st.divider()
                    else:
                        st.write()

############################## Graficos Home ################################

# Qtda de vendas enviadas para scantec

total_envios_snct = len(
    result_pdv_ori[result_pdv_ori['dados_nfce_pdv_scanntech'] == '1'])
total_envios_falhos_snct = len(
    result_pdv_ori[result_pdv_ori['dados_nfce_pdv_scanntech'] == '0'])
total_envios_nulos_snct = len(
    result_pdv_ori[result_pdv_ori['dados_nfce_pdv_scanntech'] == ''])


# st.header(total_envios_snct)
# st.header(total_envios_falhos_snct)
# st.header(total_envios_nulos_snct)

with st.container():
    st.write("Lugar")

############################################################################

# time.sleep(60)
# load_data_conc()
st_autorefresh(interval=60000, limit=100, key="fizzbuzzcounter")
# time.sleep(30)
# st.cache_resource.clear()
# st.rerun()
# st.stop()

with st.sidebar.container():
    st.divider()
    st.write()
    col1, col2, col3 = st.columns([1, 6, 1])
    with col1:
        st.write()
    with col2:
        # st.markdown("<h1 style='text-align: center; '>FRONT FLUX</h1>", unsafe_allow_html=True)
        st.image('flux_metrics_logo_M.png')
        st.info(f":closed_book: Versão - {mlogic_ver_hom}")
        # st.markdown("<h3 style='text-align: center; '>Gerencie seu Frente de Loja</h3>",unsafe_allow_html=True)
    with col3:
        st.write()
    st.divider()
# st.write(st.session_state)
# st.write('########################################################################')
