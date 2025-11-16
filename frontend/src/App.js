import React, { useState, useEffect } from 'react';
import './App.css';

// --- Le composant Card ---
// Un composant simple pour afficher les infos d'UNE entreprise
const CompanyCard = ({ company }) => {
  return (
    <div className="card">
      <h3>{company.denomination || "Nom inconnu"}</h3>
      <p>
        <strong>Numéro d'entreprise:</strong> {company.numero_entreprise}
      </p>
      <p>
        <strong>Statut:</strong> {company.statut}
      </p>
      <p>
        <strong>Adresse:</strong> {company.adresse_siege}
      </p>
      <p>
        <strong>Forme légale:</strong> {company.forme_legale}
      </p>
    </div>
  );
};

function App() {
  const [companies, setCompanies] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch('http://localhost:5001/api/companies')
      .then(response => {
        if (!response.ok) {
          throw new Error('La réponse du réseau était invalide');
        }
        return response.json();
      })
      .then(data => {
        // Pour limiter le nombre de cartes
        // setCompanies(data.slice(0, 20)); 
        setCompanies(data);
        setLoading(false);
      })
      .catch(error => {
        setError(error.message);
        setLoading(false);
      });
  }, []);

  return (
    <div className="App">
      <header className="App-header">
        <h1>Données des entreprises Belges </h1>
      </header>
      
      <div className="content">
        {loading && <p>Chargement...</p>}
        {error && <p style={{ color: 'red' }}>Erreur: {error}</p>}
        
        <div className="card-container">
          {companies.map((company, index) => (
            <CompanyCard key={company.numero_entreprise || index} company={company} />
          ))}
        </div>
      </div>
    </div>
  );
}

export default App;