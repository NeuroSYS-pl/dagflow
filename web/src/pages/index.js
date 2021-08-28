import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './index.module.css';
import HomepageFeatures from '../components/HomepageFeatures';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <h1 className="hero__title">{siteConfig.title} 👧</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/home">
            Quick Start ⏱️
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  return (
    <Layout
      title="Landing Page"
      description="MAGDA Overview">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <div className="container text--center">
          <CodeBlock className="bash">pip install magda</CodeBlock>
        </div>
      </main>
    </Layout>
  );
}
