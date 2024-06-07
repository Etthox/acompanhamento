get_id_cr = """select id,Status from estrutura where nivel = 3 and codigo in {} and status != 4"""

get_Rotina_Por_Estrutura = """select Id from Rotina with (nolock) where EstruturaId = {} and OrigemId = '...'"""

