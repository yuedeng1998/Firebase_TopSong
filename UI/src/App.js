import './App.css';
import ReactDOM from "react-dom/client";
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Command from './component/CommandLine';
import { Navbar, Nav } from 'react-bootstrap';


export default function App() {
  return (
    // <Router>
    //   <Navbar bg="light" expand="lg" i>
    //     <Navbar.Brand href="/">Home</Navbar.Brand>
    //     <Navbar.Collapse id="basic-navbar-nav">
    //       <Nav className="mr-auto">
    //         <Nav.Link href="/command">command line</Nav.Link><br></br>
    //       </Nav>
    //     </Navbar.Collapse>
    //   </Navbar>
    //   <Routes>
    //     <Route path='/command' element={<command />} />
    //   </Routes>
    // </Router>
    <Command></Command>
  );
}
const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<App />);
