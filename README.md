## How to init servers

```bash
  docker compose up -d
```

Request  

```http
  POST http://localhost:8080/migrate
``` 

| Body | Type     | Description                       |
| :-------- | :------- | :-------------------------------- |
| `city_names`      | `string` | **Optional**. the name of the city to migrate users living in it |
| `district_names`      | `string` | **Required**. the name of the district to migrate users living in it |
