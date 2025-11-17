import React, { useState, useEffect } from 'react';
import axios from 'axios';
import {
  Container, Typography, Table, TableBody, TableCell, TableContainer,
  TableHead, TableRow, Paper, Chip, CircularProgress, Alert, Box,
  TextField, Select, MenuItem, FormControl, InputLabel, Button
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';

interface PlanningItem {
  type: string;
  matricule: string;
  nom_entretien: string;
  date_prevue: string;
  statut: string;
}

function App() {
  const [planning, setPlanning] = useState<PlanningItem[]>([]);
  const [filtered, setFiltered] = useState<PlanningItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [matriculeFilter, setMatriculeFilter] = useState('');
  const [typeFilter, setTypeFilter] = useState('all');
  const [statutFilter, setStatutFilter] = useState('all');

  const fetchPlanning = async () => {
    setLoading(true);
    try {
      const res = await axios.get('/api/planning/2025');
      setPlanning(res.data);
      setFiltered(res.data);
      setError('');
    } catch (err) {
      setError("Impossible de charger le planning. L'API Flask est-elle lancée ?");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPlanning();
  }, []);

  useEffect(() => {
    let filteredData = planning;

    if (matriculeFilter) {
      filteredData = filteredData.filter(p => p.matricule.toLowerCase().includes(matriculeFilter.toLowerCase()));
    }
    if (typeFilter !== 'all') {
      filteredData = filteredData.filter(p => p.type === typeFilter);
    }
    if (statutFilter !== 'all') {
      filteredData = filteredData.filter(p => p.statut === statutFilter);
    }

    setFiltered(filteredData);
  }, [matriculeFilter, typeFilter, statutFilter, planning]);

  if (loading) return <Box display="flex" justifyContent="center" mt={10}><CircularProgress /></Box>;
  if (error) return <Container><Alert severity="error" sx={{ mt: 4 }}>{error}</Alert></Container>;

  return (
    <Container maxWidth="xl" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h3" component="h1">
          Planning Entretiens 2025
        </Typography>
        <Button variant="contained" startIcon={<RefreshIcon />} onClick={fetchPlanning}>
          Actualiser
        </Button>
      </Box>

      <Box display="flex" gap={2} mb={3} flexWrap="wrap">
        <TextField
          label="Filtrer par matricule"
          variant="outlined"
          size="small"
          value={matriculeFilter}
          onChange={(e) => setMatriculeFilter(e.target.value)}
          sx={{ minWidth: 200 }}
        />
        <FormControl size="small" sx={{ minWidth: 150 }}>
          <InputLabel>Type</InputLabel>
          <Select value={typeFilter} label="Type" onChange={(e) => setTypeFilter(e.target.value)}>
            <MenuItem value="all">Tous</MenuItem>
            <MenuItem value="Controle">Contrôle</MenuItem>
            <MenuItem value="Nettoyage">Nettoyage</MenuItem>
            <MenuItem value="Changement">Changement</MenuItem>
          </Select>
        </FormControl>
        <FormControl size="small" sx={{ minWidth: 150 }}>
          <InputLabel>Statut</InputLabel>
          <Select value={statutFilter} label="Statut" onChange={(e) => setStatutFilter(e.target.value)}>
            <MenuItem value="all">Tous</MenuItem>
            <MenuItem value="a_faire">À faire</MenuItem>
            <MenuItem value="realise">Réalisé</MenuItem>
          </Select>
        </FormControl>
      </Box>

      <TableContainer component={Paper} elevation={3}>
        <Table stickyHeader size="small">
          <TableHead>
            <TableRow sx={{ backgroundColor: '#f5f5f5' }}>
              <TableCell><strong>Date</strong></TableCell>
              <TableCell><strong>Matricule</strong></TableCell>
              <TableCell><strong>Entretien</strong></TableCell>
              <TableCell><strong>Type</strong></TableCell>
              <TableCell><strong>Statut</strong></TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {filtered.map((item, i) => (
              <TableRow key={i} hover>
                <TableCell>{item.date_prevue}</TableCell>
                <TableCell>{item.matricule}</TableCell>
                <TableCell>{item.nom_entretien}</TableCell>
                <TableCell>
                  <Chip
                    label={item.type}
                    size="small"
                    color={
                      item.type === 'Controle' ? 'info' :
                      item.type === 'Nettoyage' ? 'warning' : 'secondary'
                    }
                  />
                </TableCell>
                <TableCell>
                  <Chip
                    label={item.statut === 'realise' ? 'Réalisé' : 'À faire'}
                    size="small"
                    color={item.statut === 'realise' ? 'success' : 'default'}
                  />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <Box mt={2} textAlign="center">
        <Typography variant="body2" color="text.secondary">
          {filtered.length} / {planning.length} entretiens affichés
        </Typography>
      </Box>
    </Container>
  );
}

export default App;