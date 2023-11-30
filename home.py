import streamlit as st
import pandas as pd
import time
from sqlalchemy import create_engine as ce
############################ -----Configurações de Layout da HOME -----######################
st.set_page_config(page_title="Front FLUX", page_icon=":chart",
                   layout="wide", initial_sidebar_state="auto", menu_items=None)


########################## Funções Distintas ###############################################

def recarregar_home():
    st.experimental_rerun


############################################################################################

# Conectar ao banco de dados
username_bd = st.secrets['username']
pass_bd = st.secrets["password"]
name_bd = st.secrets["database"]
host_bd = st.secrets["host"]
# st.write(st.secrets)

mydb = ce(
    f'mysql+mysqlconnector://{username_bd}:{pass_bd}@{host_bd}/{name_bd}')
# conn = st.connection('mysql', type='sql')


############################# -----Conexão para extração de dados-----################################
conc_query = "SELECT * FROM recebe_dados_conc"
####################################################################


@st.cache_resource  # 👈 Add the caching decorator
def load_data_conc():
    result_conc = pd.read_sql(conc_query, mydb)
    return result_conc


result_conc = load_data_conc()


####################################################################
# result_conc = conn.query('SELECT * FROM recebe_dados_conc;', ttl=600)
# result_conc = pd.read_sql(conc_query, mydb)
result_conc = result_conc.sort_values(by='controlID', ascending=False)
result_conc = result_conc.drop_duplicates(subset=['rede_lojas', 'cli', 'loj', 'razsoc'])[['rede_lojas', 'cli', 'loj', 'razsoc', 'dte_atu', 'ver_flux', 'ver_jv', 'ver_pgs', 'con_ver',
                                                                                          'hd_free', 'tam_hd', 'perc_hd', 'bkp_kb_con', 'sem_id_carga', 'sem_data_carga', 'ser_sgbd', 'ser_carg_on', 'ser_tk_app', 'ser_dbridge', 'pv_con', 'uv_con', 'integracao_notas_dr', 'integracao_notas_mc']]
result_conc_ori = result_conc[['rede_lojas', 'cli', 'loj', 'razsoc', 'dte_atu', 'ver_flux', 'ver_jv', 'ver_pgs', 'con_ver', 'hd_free', 'tam_hd',
                               'perc_hd', 'bkp_kb_con', 'sem_id_carga', 'sem_data_carga', 'ser_sgbd', 'ser_carg_on', 'ser_tk_app', 'ser_dbridge', 'pv_con', 'uv_con', 'integracao_notas_dr', 'integracao_notas_mc']]

conc_rede_lojas = result_conc_ori['rede_lojas'].unique()
conc_cliente = result_conc['cli']
conc_loja = result_conc_ori['loj'].unique()
conc_razsoc = result_conc['razsoc']


############################# ----- Conexão com PDV´s ----- #############################
pdv_query = "SELECT * FROM recebe_dados_pdv "


@st.cache_resource  # 👈 Add the caching decorator
def load_data_pdv():
    result_pdv = pd.read_sql(pdv_query, mydb)
    return result_pdv


result_pdv = load_data_pdv()


# result_pdv = pd.read_sql(pdv_query, mydb)
result_pdv = result_pdv.sort_values(by='ID', ascending=False)
result_pdv = result_pdv.drop_duplicates(
    subset=['cli', 'loj', 'pdv'])[['rede_lojas', 'cli', 'loj', 'pdv', 'pdv_at', 'pdv_ve', 'ult_ca', 'pdv_at_id', 'pdv_rej', 'dados_nfce_pdv_numero', 'dados_nfce_pdv_situacao', 'dados_nfce_pdv_valor', 'dados_nfce_pdv_usuario', 'dados_nfce_pdv_enviado', 'dados_nfce_pdv_data_fech', 'dados_nfce_pdv_scanntech', 'dados_nfce_pdv_email', 'dados_nfce_pdv_url_code', 'dados_nfce_pdv_nfemissao']]
pdvs = result_pdv[['rede_lojas', 'pdv', 'loj', 'cli']]

# st.write(pdvs)

########################### Variáveis de aviso #########################
result_conc_filter = pd.unique(result_conc["cli"])
result_pdv_filter = pd.unique(result_pdv['loj'])

###############################################################
# st.table(result_pdv_erros)
sum_pdvs = len(pdvs)
sum_lojas = len(result_conc.loj)
conta_clientes = result_conc['rede_lojas'].unique()
sum_clientes = len(conta_clientes)


#######################################################
st.markdown("<h2 style='text-align: center; '>FRONTFLUX</h2>",
            unsafe_allow_html=True)
st.markdown("<h6 style='text-align: center; '>Gerencie seu Frente de Loja</h6>",
            unsafe_allow_html=True)

########################################################################
with st.container():
    col1, col2, col3, col4, col5, col6 = st.columns([1, 1, 1, 1, 1, 1])
    with col1:
        st.subheader(f"Clientes: {sum_clientes}")
    with col2:
        st.subheader(f"Lojas: {sum_lojas}")
    with col3:
        st.subheader(f"PDV: {sum_pdvs}")
    with col4:
        st.write()
    with col5:
        st.write()
    with col6:
        st.info(":closed_book: Versão Homologada - 14.5.1")

    # st.markdown("<h4 style='text-align: center; '>Layout de Erros</h4>", unsafe_allow_html=True)

########################### NOTIFICAÇÕES ###################################
with st.container():

    col01, col02, col03, col04 = st.columns(4)
    ########################################################################
    with col01:
        st.caption("BANCO DE DADOS")
        notifica_erros_sgbd = result_conc_ori[result_conc_ori['ser_sgbd'] == 'Stopped']
        notifica_erros_sgbd = notifica_erros_sgbd[[
            'rede_lojas', 'razsoc', 'ser_sgbd']]
        # notifica_erros_carga_rede_sgbd = notifica_erros_sgbd['rede_lojas'].to_list()[0]
        for i1, grupo1 in notifica_erros_sgbd.groupby('razsoc'):
            sgbd_erros_rede = i1
            if not notifica_erros_sgbd.empty:
                st.error(
                    f'{sgbd_erros_rede}', icon="🚨")
            else:
                st.write("SEM ERROS PARA APRESENTAR")
    ########################################################################
    with col02:
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
            else:
                st.write("SEM ERROS PARA APRESENTAR")
    ########################################################################
    with col03:
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
    with col04:
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

            # st.write(grupo4)

########################### SIDEBAR DE OPÇÕES ##############################

with st.sidebar:
    selecao_rede_me = st.selectbox(
        'Rede', conc_rede_lojas, index=None, placeholder="Escolha o Cliente")

    conc_dados_completos = result_conc_ori[result_conc_ori['rede_lojas']
                                           == f"{selecao_rede_me}"]
    # selecao_loja_me = st.checkbox('Lojas: ', conc_dados_completos['loj'], index=0, placeholder="Choose an option")


########################### Verificação de Notas Integradas ##############################


# Layout para apresentação de dados da Central e PDV´s
with st. container():

    st.divider()

    for cli, grupoconc in conc_dados_completos.groupby('cli'):

        # st.table(grupoconc)

        nome_loja_checkbox = grupoconc['razsoc'].to_list()[0]
        checkbox_selecao_loja = st.sidebar.checkbox(
            f'{nome_loja_checkbox} ', value=False, key=conc_cliente+cli)
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
                ####### LAYOUT DE APRESENTAÇÃO DE DADOS DA CENTRAL ###########
                col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14 = st.columns(
                    [2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])

                with col1:
                    st.caption(
                        f"{lojas_rede} || {flux_ve} || PDV: {total_pdvs_loja}")
                    st.info(f"{raz_loja} ")
                    st.caption(
                        f"Atualizado em: {atu_conc}")
                with col2:
                    st.caption("Central")
                    st.info(f"{con_ve}")
                with col3:
                    st.caption("JAVA")
                    st.info(f"{java_ve}")
                with col4:
                    st.caption("HD")
                    if hd_livre <= '40':
                        st.success(f"{hd_livre} %")
                    elif hd_livre > '40' < '70':
                        st.warning(f"{hd_livre} %")
                    else:
                        st.error(f"{hd_livre} %")
                with col5:
                    st.caption("BKP CONC")
                    st.info(f"{tam_bkp_conc}\GB")

                with col6:
                    st.caption("Carga")
                    st.info(f"{data_carga_conc}")
                with col7:
                    st.caption("IDCARGA")
                    st.info(f"{id_carga_conc}")
                with col8:
                    st.caption("SGBD")
                    if 'Stopped' in lista_dados_conc['ser_sgbd'].to_list():
                        st.error("SGBD :rotating_light:")
                    elif 'Running' in lista_dados_conc['ser_sgbd'].to_list():
                        st.success(f"{pgs_ve}")
                    else:
                        st.warning(f"{pgs_ve}")
                with col9:
                    st.caption("Mlogic")
                    if 'Stopped' in lista_dados_conc['ser_carg_on'].to_list():
                        st.error("Mlogic :rotating_light:")
                    elif 'Running' in lista_dados_conc['ser_carg_on'].to_list():
                        st.success(f"OK")
                    else:
                        st.warning("Verificar")
                with col10:
                    st.caption("NFCe")
                    if 'Stopped' in lista_dados_conc['ser_tk_app'].to_list():
                        st.error("TK :rotating_light:")
                    elif 'Running' in lista_dados_conc['ser_tk_app'].to_list():
                        st.success(f"OK")
                    else:
                        st.warning("Verificar")
                with col11:
                    st.caption("Bridge")
                    if 'Stopped' in lista_dados_conc['ser_dbridge'].to_list():
                        st.error("Bridge :rotating_light:")
                    elif 'Running' in lista_dados_conc['ser_dbridge'].to_list():
                        st.success(f"OK")
                    else:
                        st.warning("Verificar")
                with col12:
                    st.caption("Integradas")
                    st.info(f"{integracao_notas_mc}")
                with col13:
                    st.caption("Pendentes")
                    if integracao_notas_dr == "0":
                        st.success(f"{integracao_notas_dr}")
                    else:
                        st.error(f"{integracao_notas_dr}")
                with col14:
                    st.caption("Ver PDVs")
                    mostrar_pdvs = st.toggle(f'PDV', key=id_cli+id_loja)
                with st.empty():

                    if mostrar_pdvs:
                        ########################################################################
                        pdv_dados_completos = result_pdv[result_pdv['cli'] == cli]

                        with st.container():
                            st.divider()
                            st.subheader(f'FRENTE DE LOJA = {raz_loja}')
                            st.divider()

                            # isall = st.sidebar.checkbox(label="Selecionar Todos", value=True, key=id_cli+hd_livre)
                            for (pdv, cli), grupopdv in pdv_dados_completos.groupby(['pdv', 'cli']):

                                # st.table(grupopdv)

                                # info_pdv_dados = st.sidebar.checkbox(f"{pdv}", value=isall)
                                # if info_pdv_dados:
                                lista_dados_pdv = pdv_dados_completos[pdv_dados_completos['pdv'] == pdv]
                                lista_dados_pdv['ult_ca'] = pd.to_datetime(
                                    lista_dados_pdv['ult_ca'])
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
                                dados_nfce_pdv_data_fech = lista_dados_pdv['dados_nfce_pdv_data_fech'].to_list()[
                                    0]
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
                                col01, col02, col03, col04, col05, col06, col07, col08, col09 = st.columns(
                                    [1, 1, 1, 1, 1, 1, 1, 1, 1])

                                with col01:
                                    st.caption('PDV')
                                    st.info(f"{id_pdv}")
                                with col02:
                                    st.caption('Scanntech')
                                    if (f"{dados_nfce_pdv_scanntech}") != '1':
                                        st.error(f"OFF")
                                    else:
                                        st.success(f"Online")

                                with col03:
                                    st.caption('CARGA')
                                    if (f"{pdv_carg}") != data_carga_conc:
                                        st.error(f"{pdv_carg}")
                                    else:
                                        st.success(f"{pdv_carg}")
                                with col04:
                                    st.caption('ID')
                                    if (f"{id_pdv_carg}") != id_carga_conc:
                                        st.error(f'{id_pdv_carg}')
                                    else:
                                        st.success(f"{id_pdv_carg}")

                                with col05:
                                    st.caption('VERSÃO')
                                    if (f"{ve_pdv}") != con_ve:
                                        st.error(f"{ve_pdv}")

                                    else:
                                        st.success(f"{ve_pdv}")
                                with col06:
                                    st.caption('Checkout')
                                    st.info(
                                        f" {dados_nfce_pdv_data_fech}")
                                    st.caption("Tempo Ocioso")
                                with col07:
                                    st.caption('Nº NFCe')
                                    st.info(
                                        f"{dados_nfce_pdv_numero}")
                                with col08:
                                    st.caption('SEFAZ')
                                    if (f"{pdv_rej}") == '100':
                                        st.success(f"{pdv_rej}")
                                    elif (f"{pdv_rej}") != '100':
                                        st.warning(f"Verificando")
                                    else:
                                        st.error(f"{pdv_rej}")
                                with col09:
                                    st.caption("VER +")
                                    detalhes_do_pdv = st.toggle(
                                        f'ON/OFF', key=id_cli+id_pdv)

                                with st.container():
                                    st.divider()
                                    if detalhes_do_pdv:
                                        col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(
                                            [2, 1, 1, 1, 1, 1, 1, 1])
                                        with col1:
                                            st.caption('Usuario')
                                            st.info(
                                                f'{dados_nfce_pdv_usuario}')

                                        with col2:
                                            st.caption('Nº NFCe')
                                            st.info(
                                                f'{dados_nfce_pdv_numero}')

                                        with col3:
                                            st.caption('CARGA')
                                            st.info(f'{id_pdv_carg}')

                                        with col4:
                                            st.caption('VALOR')
                                            st.info(
                                                f"{dados_nfce_pdv_valor}")
                                            st.caption('IMPRESSORA')
                                            if (f"{dados_nfce_pdv_nfemissao}") != "1":
                                                st.error(f'Falha')
                                            else:
                                                st.success(f"OK")

                                        with col5:
                                            st.caption('ENVIADO')
                                            if (f"{dados_nfce_pdv_enviado}") != "1":
                                                st.error(f'Não')
                                            else:
                                                st.success(f"Sim")

                                            st.caption('EMAIL')
                                            if (f"{dados_nfce_pdv_email}") != '1':
                                                st.error(f'Não')
                                            else:
                                                st.success(f"Sim")

                                        with col6:
                                            st.caption('TEF')
                                            st.success('OK')
                                            st.caption('SEFAZ')
                                            st.success('OK')

                                        with col7:
                                            st.caption('NFCE')
                                            st.link_button(
                                                "Ver", f"{dados_nfce_pdv_url_code}", type="secondary")

                                        st.divider()
                                    else:
                                        st.write()
                            st.sidebar.divider()
                    else:
                        st.write()
                st.divider()

# st.experimental_memo
# st.runtime.legacy_caching.clear_cache()
# st.write(st.session_state)
# st.write('########################################################################')
