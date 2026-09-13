---
name: discordia-dod-checklist
description: Checklist de Definition of Done y de pre-PR del equipo Discordia, aplicado a discordia-chat — cobertura, secretos, catálogo de eventos, contrato OpenAPI, docker-compose, README. Usar antes de pedir review de un PR o al dar una historia por terminada.
---

# Checklist antes de pedir review / dar una historia por terminada

Fuente: `discordia-docs/procesos/definition-of-done.md` y
`procesos/git-workflow.md`.

## Código

- [ ] Cumple los criterios de aceptación de la historia en Jira.
- [ ] CI en verde (CI roto es red line de la consigna).
- [ ] `make test-cover` — cobertura ≥ 70% y **no bajó** respecto de antes del
      PR.
- [ ] Sin `.env`, credenciales ni claves en el diff (revisar `git status`
      antes de `git add`).
- [ ] El PR toca una sola historia de Jira y tiene un reviewer que no es el
      autor.

## Integración

- [ ] Si el PR agrega o cambia un evento que `chat` publica o consume, está
      reflejado en `discordia-docs/arquitectura/eventos.md` en el mismo PR
      (ver también `discordia-event-idempotency` y
      `discordia-rabbitmq-topology` para la implementación).
- [ ] Si expone un endpoint HTTP nuevo, está en el contrato OpenAPI del
      servicio.
- [ ] Levanta con el `docker-compose` compartido sin pasos manuales extra.

## Documentación

- [ ] Si hubo una decisión de diseño (tecnología, contrato entre servicios),
      quedó en un ADR — ver `discordia-adr-routing` para decidir si va en
      `discordia-docs/adr` o en el `adr/` de este servicio.
- [ ] El README dice cómo correr el servicio y qué variables de entorno
      necesita (`.env.example` actualizado si se agregó una nueva).
- [ ] La historia está en el estado correcto en Jira.

## Lo que NO es parte de "done"

- Estar desplegado en la nube — eso pasa en el corte a `master` de cada
  checkpoint.
- Estar demostrado en la reunión — eso es la revisión del sprint.
