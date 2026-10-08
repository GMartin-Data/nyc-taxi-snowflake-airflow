# ADR-0003 : Masquer l'identifiant du compte Snowflake sur les captures publiées

Status: Accepted
Date: 2026-10-08

## Contexte

Le dépôt est public et le README affirme qu'aucun secret n'est dans Git :
l'utilisateur de service s'authentifie par paire de clés, la clé privée reste
sur le poste, `SNOWFLAKE_ACCOUNT` vit dans un `.env` ignoré, gitleaks bloque un
commit qui contiendrait une clé.

La revue de la PR #5 a relevé que la capture Cost Management des crédits
(`docs/captures/snowflake_credits.png`, commit `eaf005f`) montre le filtre de
compte avec le locator du compte d'essai, le coin utilisateur avec l'admin et
le solde, et des identifiants de requêtes. Le locator n'authentifie pas : la
clé reste requise. Mais il forme le préfixe de l'URL de connexion, il réduit ce
qu'un attaquant doit deviner pour viser `AIRFLOW_SVC` ou `ACCOUNTADMIN`, et
Git le conserve indéfiniment une fois poussé. La capture contredit ainsi la
règle que le dépôt s'est donnée, alors même que le README affirme cette règle.

Le périmètre est vérifié : le locator n'apparaît dans aucun fichier texte de
l'historique ; la seule autre capture Snowsight déjà fusionnée
(`droits_role_transformer.png`) ne montre aucun identifiant de compte ; la
branche de la PR #5 n'est pas fusionnée et les deux commits qui suivent
`eaf005f` ne touchent aucune capture.

## Options considérées

- **Option A — masquer avant commit** : toute capture Snowsight est recadrée ou
  floutée avant d'être commitée ; le locator, le nom de compte et l'utilisateur
  admin rejoignent les valeurs tenues hors de Git. Cohérente avec le `.env`
  ignoré et la promesse du README ; valable sur un futur compte qui ne sera
  pas jetable. Coût : un geste de plus par capture, et le PNG déjà poussé est
  à refaire.
- **Option B — assumer le locator** : le locator d'un compte d'essai jetable est
  jugé non sensible ; la règle « aucun secret » est précisée (les secrets sont
  les clés et les mots de passe, pas les identifiants). Coût nul aujourd'hui ;
  mais la règle ne tient que tant que le compte est un compte d'essai, et le
  README devrait être nuancé.

Pour le PNG déjà poussé, deux gestes sont possibles quelle que soit l'option :
un commit de remplacement, qui laisse l'ancien blob dans l'historique de la
branche, ou la réécriture de la branche avant la fusion, qui le retire de la
chaîne des commits.

## Décision

Option A, avec réécriture de la branche avant la fusion. Le coût est faible :
un seul PNG à recadrer, un seul commit à amender, deux commits à rejouer sans
conflit possible. L'option B est écartée parce qu'elle fait dépendre une règle
de dépôt de la nature jetable du compte courant, et qu'elle oblige à affaiblir
la phrase du README plutôt qu'à la tenir. Le commit de remplacement est écarté
parce qu'il laisserait le locator dans un historique que la fusion rendrait
définitif sur `main`.

## Conséquences

- Règle pour toute capture Snowsight : l'en-tête de compte, le filtre de compte
  et le coin utilisateur sont masqués avant le commit `docs:`. Le recadrage
  Pillow déjà pratiqué pour les captures s'en charge.
- `snowflake_credits.png` est réduit au titre et au graphe par jour : la barre
  de filtres, la colonne de gauche et la table des requêtes sont retirées, la
  table portant le nom de l'utilisateur admin dans sa colonne USER. Puis
  `eaf005f` est amendé et les deux commits suivants rejoués dessus :
  `git rebase --onto`. Le nom du fichier ne change pas, le README reste
  valide. Critère : six commits sur `main..HEAD`, mêmes messages, PR #5
  `MERGEABLE` sur le nouveau SHA.
- Le force-push de la branche est un geste de l'humain, avec `!` : le hook du
  poste le bloque pour l'assistant, par construction.
- Trade-off accepté : GitHub garde l'ancien commit accessible par son SHA
  pendant un temps indéterminé, et l'événement « force-pushed » de la PR y
  renvoie ; seul le support GitHub purge vraiment. Le reflog local le garde
  jusqu'au prochain `git gc`. Hors de proportion pour un compte d'essai qui
  expire vers le 2026-11-04 : la réécriture est un geste de propreté qui rend
  le dépôt conforme à sa règle, pas une purge.
- La règle vaut pour les captures à venir du bonus Astro hébergé, où l'en-tête
  du Deployment porte des identifiants d'organisation.
