from bs4 import BeautifulSoup
import re

class ParsingHTML:
    def parse_kbo_html(html_content: str) -> dict:
        """
        Parse le contenu HTML d'une page KBO/BCE et extrait les données de base.
        
        :param html_content: Le contenu HTML brut de la page.
        :return: Un dictionnaire contenant les données extraites.
        """
        soup = BeautifulSoup(html_content, 'lxml')
        data = {}
        main_table = soup.find('div', id='table')
        if not main_table:
            return {"error": "Main table not found"}

        labels_to_find = {
            "Numéro d'entreprise:": "numero_entreprise",
            "Statut:": "statut",
            "Situation juridique:": "situation_juridique",
            "Date de début:": "date_debut",
            "Dénomination:": "denomination",
            "Adresse du siège:": "adresse_siege",
            "Forme légale:": "forme_legale"
        }

        for label_text, data_key in labels_to_find.items():
            try:
                label_cell = main_table.find('td', string=re.compile(r'\s*' + re.escape(label_text) + r'\s*'))
                if label_cell:
                    value_cell = label_cell.find_next_sibling('td')
                    if value_cell:
                        for span_upd in value_cell.find_all('span', class_='upd'):
                            span_upd.decompose()
                        for br in value_cell.find_all('br'):
                            br.replace_with(' ')
                        for sup in value_cell.find_all('sup'):
                            sup.decompose()
                        
                        value = value_cell.get_text(strip=True).replace('\xa0', ' ')
                        value = re.sub(r'\s+', ' ', value).strip()
                        data[data_key] = value
            except Exception as e:
                print(f"Erreur lors du parsing du label '{label_text}': {e}")
                data[data_key] = None
                
        return data
