---
name: discordia-dod-checklist
description: Checklist de Definition of Done y de pre-PR del equipo Discordia, aplicado al cliente compartido de Pub/Sub — paridad entre los dos lenguajes, cobertura con el broker levantado, secretos, fixtures de interoperabilidad y README. Usar antes de pedir review de un PR o al dar una historia por terminada.
---

# Checklist antes de pedir review

Fuente: `discordia-docs/procesos/definition-of-done.md` y
`procesos/git-workflow.md`.

## Propio de este repo

- [ ] **Paridad entre los dos clientes.** Un cambio de comportamiento en Go
      va también en Python, en el mismo PR. Los servicios de los dos
      lenguajes tienen que ver el mismo bus.
- [ ] Si cambió la forma del JSON del sobre, se regeneraron los fixtures de
      `testdata/` y pasan los tests de interoperabilidad de los dos lados.
- [ ] Si se agregó una variable de entorno, está en `.env.example`, en la
      tabla del README y en los dos clientes con el mismo nombre y el mismo
      default.
- [ ] Si se tocó la declaración de colas, los tests que distinguen cola
      compartida de cola por instancia siguen en verde **sin aflojar la
      aserción** (ver `discordia-rabbitmq-topology`).

## Código

- [ ] Cumple los criterios de aceptación de la historia en Jira.
- [ ] CI en verde (CI roto es red line de la consigna).
- [ ] `make test-all` con el broker levantado. La cobertura se mide así: sin
      broker los tests de integración se saltan y el número no es real.
- [ ] Cobertura ≥ 70% y **no bajó** respecto de antes del PR.
- [ ] Sin `.env`, credenciales ni claves en el diff. Tampoco en los defaults
      del código.
- [ ] El PR toca una sola historia de Jira y tiene un reviewer que no es el
      autor.

## Documentación

- [ ] Si hubo una decisión de diseño, quedó en un ADR o en el README — ver
      `discordia-adr-routing` para decidir dónde.
- [ ] El README explica cómo usar lo que se agregó, no solo que existe.

## Lo que NO es parte de "done"

- Estar desplegado en la nube — eso pasa en el corte a `master` de cada
  checkpoint.
- Estar demostrado en la reunión — eso es la revisión del sprint.
