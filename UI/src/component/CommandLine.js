import React, { Component } from 'react'
import "./CommandLine.css";

const Command = () => {
    return (
        <div>
            <form className="main-form" action="/" method="get">
                <label htmlFor="header-search">
                    <div className="visually-hidden">Insert Command</div>
                </label>
                <input
                    type="text"
                    className="main-input"
                    id="header-search"
                    placeholder="Insert Command Here"
                    name="s" 
                />
                <button type="submit">Operate</button>
            </form>            
        </div>
    )}

export default Command