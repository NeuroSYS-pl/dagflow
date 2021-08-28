import React from 'react';
import clsx from 'clsx';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faCubes, faStopwatch, faProjectDiagram } from '@fortawesome/free-solid-svg-icons';
import styles from './HomepageFeatures.module.css';

const FeatureList = [
  {
    title: 'Modular',
    icon: faCubes,
    description: (
      <>
        It is designed to divide code into small logical blocks (modules) with explicit
        input and output, following the simple rule <code>1 module = 1 role</code>.
      </>
    ),
  },
  {
    title: 'Asynchronous',
    icon: faStopwatch,
    description: (
      <>
        The library is based on asyncio and ray, which allows it to run
        modules simultaneously. This gives you a simple optimization out of the box.
      </>
    ),
  },
  {
    title: 'with Directed and Acyclic Graphs (DAG)',
    icon: faProjectDiagram,
    description: (
      <>
        Modules are joined together without any cycles into 1 stream. You can think
        of modules as graph nodes and focus solely on their role and connections.
      </>
    ),
  },
];

function Feature({icon, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className={clsx('text--center', styles.featureIcon)}>
        <FontAwesomeIcon icon={icon} />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
